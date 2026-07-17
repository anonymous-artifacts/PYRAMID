// PartialRDS_GC_host.cpp — Dynamic Graph Coloring with Phase 1 offloaded to DPU
// Phase 1 (ProcessCE): DPU via TCP
// Phase 2 (CheckConflict): CPU
// Phase 3 (UpdateNeighbors): CPU
//
// Compile: g++ -O3 -fopenmp -std=c++17 PartialRDS_GC_host.cpp -o PartialRDS_GC_host
// Run:     ./PartialRDS_GC_host <MTX> <Colors> <BatchesFolder> <NumBatches> <Threads> <DPU_Host> [DPU_Port]

#include <omp.h>
#include <iostream>
#include <vector>
#include <fstream>
#include <sstream>
#include <chrono>
#include <algorithm>
#include <numeric>
#include <random>
#include <iomanip>
#include <cmath>
#include <cstring>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <unistd.h>
#include <stdint.h>
#include <errno.h>
#include <sys/time.h>

using namespace std;

#define MAX_COLORS 1024
#define SHUTDOWN_SENTINEL 0xFFFFFFFF

int n_threads = 32;

// ============================================================================
//                          DATA STRUCTURES
// ============================================================================

struct Node {
    int label;
    int color;
    int color_prev;

    Node(int l = 0, int c = -1) : label(l), color(c), color_prev(-1) {}
    int getColor() const { return color; }
    void setColor(int c) { color = c; }
    int getPrevColor() const { return color_prev; }
    void setPrevColor(int c) { color_prev = c; }
};

class Graph {
public:
    int num_nodes;
    long long num_edges = 0;
    vector<vector<int>> adj_list;

    Graph(int n = 0) : num_nodes(n), adj_list(n) {}

    void addEdge(int u, int v) {
        if (u >= 0 && u < num_nodes && v >= 0 && v < num_nodes) {
            adj_list[u].push_back(v);
            adj_list[v].push_back(u);
            num_edges++;
        }
    }

    void removeEdge(int u, int v) {
        if (u >= 0 && u < num_nodes && v >= 0 && v < num_nodes) {
            auto& nU = adj_list[u];
            nU.erase(remove(nU.begin(), nU.end(), v), nU.end());
            auto& nV = adj_list[v];
            nV.erase(remove(nV.begin(), nV.end(), u), nV.end());
            num_edges--;
        }
    }

    bool isAdjacent(int u, int v) const {
        if (u < 0 || u >= num_nodes) return false;
        for (int nbr : adj_list[u]) if (nbr == v) return true;
        return false;
    }
};

struct Edge {
    int source, destination;
    bool isInsertion;
    Edge(int s = 0, int d = 0, bool ins = false) : source(s), destination(d), isInsertion(ins) {}
};

// DPU batch edge (matches DPU's BatchEdge struct)
struct BatchEdge { uint32_t src, dst, is_ins; };

// Color sync entry sent to DPU
struct ColorSync { uint32_t node_id; int32_t new_color; };

// Color change received from DPU
struct ColorChange { uint32_t node_id; int32_t old_color, new_color; };

struct BatchMetrics {
    int id;
    string filename;
    float PrePhase_CPU;
    float P1_DPU;
    float P1_Transfer;
    float P1_Total;
    float CheckConflict_CPU_P2;
    float UpdateNeighbors_CPU_P3;
    float Verify_CPU_post;
    int iterations;
    int chromatic_num;
    float time_total_batch;
};

// ============================================================================
//                          NETWORK HELPERS
// ============================================================================

static int recv_all(int fd, void *buf, size_t len) {
    size_t r = 0;
    while (r < len) {
        ssize_t n = recv(fd, (char *)buf + r, len - r, 0);
        if (n < 0) { cerr << "[Host] recv error: " << strerror(errno) << endl; return -1; }
        if (n == 0) { cerr << "[Host] DPU closed connection" << endl; return -1; }
        r += n;
    }
    return 0;
}

static int send_all(int fd, const void *buf, size_t len) {
    size_t s = 0;
    while (s < len) {
        ssize_t n = send(fd, (const char *)buf + s, len - s, 0);
        if (n <= 0) { cerr << "[Host] send error: " << strerror(errno) << endl; return -1; }
        s += n;
    }
    return 0;
}

static int connect_to_dpu(const char *host, int port, int timeout_sec = 5) {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);

    struct hostent *he = gethostbyname(host);
    if (!he) { cerr << "[Host] Cannot resolve " << host << endl; close(sock); return -1; }
    memcpy(&addr.sin_addr, he->h_addr_list[0], he->h_length);

    int bufsize = 4 * 1024 * 1024;
    setsockopt(sock, SOL_SOCKET, SO_SNDBUF, &bufsize, sizeof(bufsize));
    setsockopt(sock, SOL_SOCKET, SO_RCVBUF, &bufsize, sizeof(bufsize));

    // Set connection timeout
    struct timeval tv;
    tv.tv_sec = timeout_sec;
    tv.tv_usec = 0;
    setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

    cout << "[Host] Connecting to DPU at " << host << ":" << port
         << " (timeout " << timeout_sec << "s)..." << endl;
    if (connect(sock, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        cerr << "[Host] Connection failed: " << strerror(errno) << endl;
        cerr << "[Host] Make sure PartialRDS_GC_dpu is running on the DPU device." << endl;
        close(sock); return -1;
    }
    // Reset timeout for data transfer
    tv.tv_sec = 0; tv.tv_usec = 0;
    setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
    cout << "[Host] Connected to DPU." << endl;
    return sock;
}

// ============================================================================
//                     SEND INITIAL GRAPH TO DPU
// ============================================================================

static int send_initial_graph(int sock, const Graph &graph, const vector<Node> &nodes) {
    uint32_t n = graph.num_nodes;

    // Build CSR
    vector<uint32_t> offsets(n + 1, 0);
    for (uint32_t i = 0; i < n; i++)
        offsets[i + 1] = offsets[i] + (uint32_t)graph.adj_list[i].size();
    uint32_t ne = offsets[n];

    vector<uint32_t> nbrs;
    nbrs.reserve(ne);
    for (uint32_t i = 0; i < n; i++)
        for (int nbr : graph.adj_list[i])
            nbrs.push_back((uint32_t)nbr);

    // Colors as int32_t
    vector<int32_t> colors_arr(n);
    for (uint32_t i = 0; i < n; i++)
        colors_arr[i] = nodes[i].getColor();

    if (send_all(sock, &n, 4) < 0) return -1;
    if (send_all(sock, &ne, 4) < 0) return -1;
    if (send_all(sock, offsets.data(), (size_t)(n + 1) * 4) < 0) return -1;
    if (ne > 0 && send_all(sock, nbrs.data(), (size_t)ne * 4) < 0) return -1;
    if (send_all(sock, colors_arr.data(), (size_t)n * 4) < 0) return -1;

    return 0;
}

// ============================================================================
//                     COLORING UTILITY FUNCTIONS
// ============================================================================

int getChromaticNumber(const vector<Node>& nodes) {
    int max_c = -1;
    for (const auto& n : nodes) if (n.getColor() > max_c) max_c = n.getColor();
    return max_c + 1;
}

bool verifyColoring(const Graph& graph, const vector<Node>& nodes, const string& phase) {
    long long conflicts = 0;
    int uncolored = 0;

    #pragma omp parallel for num_threads(n_threads) reduction(+:conflicts, uncolored)
    for (int u = 0; u < graph.num_nodes; u++) {
        int cu = nodes[u].getColor();
        if (cu == -1) { uncolored++; continue; }
        for (int v : graph.adj_list[u])
            if (u < v && nodes[v].getColor() == cu) conflicts++;
    }

    if (conflicts > 0 || uncolored > 0) {
        cout << "[FAILED] " << phase << ": " << conflicts << " conflicts, "
             << uncolored << " uncolored." << endl;
        return false;
    }
    return true;
}

int getSmallestAvailableColor_Naive(int u, const Graph& graph, const vector<Node>& nodes) {
    for (int c = 0; c < MAX_COLORS; c++) {
        bool is_used = false;
        for (int v : graph.adj_list[u])
            if (nodes[v].getColor() == c) { is_used = true; break; }
        if (!is_used) return c;
    }
    return MAX_COLORS;
}

void runFullGreedyRepair(Graph& graph, vector<Node>& nodes) {
    cout << ">>> STARTING GREEDY REPAIR..." << endl;
    while (true) {
        int fixed = 0;
        for (int u = 0; u < graph.num_nodes; u++) {
            bool conflict = (nodes[u].getColor() == -1);
            if (!conflict) {
                for (int v : graph.adj_list[u])
                    if (nodes[v].getColor() == nodes[u].getColor() && u < v) conflict = true;
            }
            if (conflict) {
                vector<int> used;
                for (int v : graph.adj_list[u])
                    if (nodes[v].getColor() >= 0) used.push_back(nodes[v].getColor());
                sort(used.begin(), used.end());
                int c = 0;
                for (int val : used) { if (val == c) c++; else if (val > c) break; }
                nodes[u].setColor(c);
                nodes[u].setPrevColor(c);
                fixed++;
            }
        }
        if (fixed == 0) break;
    }
}

// ============================================================================
//               PHASE 2: CheckConflict
// ============================================================================

vector<int> CheckConflict(const Graph& graph, vector<Node>& nodes, int v) {
    vector<int> A;
    int cv = nodes[v].getColor();
    for (int u : graph.adj_list[v]) {
        if (nodes[u].getColor() == cv) {
            int y = (u > v ? u : v);
            int old = nodes[y].getColor();
            nodes[y].setPrevColor(old);
            int new_color = getSmallestAvailableColor_Naive(y, graph, nodes);
            nodes[y].setColor(new_color);
            A.push_back(y);
        }
    }
    return A;
}

// ============================================================================
//               PHASE 3: UpdateNeighbors
// ============================================================================

vector<int> UpdateNeighbors(const Graph& graph, vector<Node>& nodes, int v) {
    vector<int> A;
    int freed_color = nodes[v].getPrevColor();
    if (freed_color < 0) return A;

    for (int u : graph.adj_list[v]) {
        if (freed_color < nodes[u].getColor()) {
            bool in_SC = false;
            for (int nbr : graph.adj_list[u])
                if (nodes[nbr].getColor() == freed_color) { in_SC = true; break; }
            if (!in_SC) {
                int old = nodes[u].getColor();
                nodes[u].setPrevColor(old);
                nodes[u].setColor(freed_color);
                A.push_back(u);
            }
        }
    }
    return A;
}

// ============================================================================
//                     BATCH SELECTION (fixed seed)
// ============================================================================

int countBatchFiles(const string& folder) {
    int count = 0;
    for (int i = 1; i <= 1000; i++) {
        string path = folder + "/" + to_string(i) + ".mtx";
        ifstream f(path);
        if (f.good()) count++;
        else if (i > count + 5) break;
    }
    return count;
}

vector<int> selectBatches(int total, int select, unsigned seed = 42) {
    vector<int> indices(total);
    iota(indices.begin(), indices.end(), 1);
    mt19937 rng(seed);
    shuffle(indices.begin(), indices.end(), rng);
    indices.resize(min(select, total));
    sort(indices.begin(), indices.end());
    return indices;
}

// ============================================================================
//                              MAIN
// ============================================================================

int main(int argc, char* argv[]) {
    if (argc < 7) {
        cerr << "Usage: " << argv[0]
             << " <MTX> <Colors> <BatchesFolder> <NumBatches> <Threads> <DPU_Host> [DPU_Port]" << endl;
        return 1;
    }

    string mtx_path = argv[1];
    string color_path = argv[2];
    string batch_folder = argv[3];
    int num_batches = stoi(argv[4]);
    n_threads = stoi(argv[5]);
    const char *dpu_host = argv[6];
    int dpu_port = (argc > 7) ? stoi(argv[7]) : 5000;

    cout << "==================== PartialRDS_GC ====================" << endl;
    cout << "MTX:      " << mtx_path << endl;
    cout << "Colors:   " << color_path << endl;
    cout << "Batches:  " << batch_folder << endl;
    cout << "NumBatch: " << num_batches << endl;
    cout << "Threads:  " << n_threads << endl;
    cout << "DPU:      " << dpu_host << ":" << dpu_port << endl;
    cout << "=======================================================" << endl;

    // ---- Load Graph ----
    Graph graph;
    {
        ifstream mtx(mtx_path);
        if (!mtx.is_open()) { cerr << "ERROR: Cannot open " << mtx_path << endl; return 1; }
        string line;
        while (getline(mtx, line)) {
            if (line.empty() || line[0] == '%') continue;
            break;
        }
        stringstream ss(line);
        long long n_ll, cols_ll, e_ll;
        ss >> n_ll >> cols_ll >> e_ll;
        int n = (int)n_ll;
        graph.num_nodes = n;
        graph.adj_list.resize(n);
        while (getline(mtx, line)) {
            if (line.empty()) continue;
            stringstream ess(line);
            int u, v;
            if (ess >> u >> v) {
                u--; v--;  // MTX is 1-indexed
                if (u >= 0 && u < n && v >= 0 && v < n)
                    graph.addEdge(u, v);
            }
        }
    }
    int n = graph.num_nodes;

    // Deduplicate adjacency lists (MTX has both directions → addEdge creates duplicates)
    // Critical: DPU's adj_rem only removes one copy, so CSR must be clean
    {
        long long deduped_edges = 0;
        for (int i = 0; i < n; i++) {
            auto &al = graph.adj_list[i];
            sort(al.begin(), al.end());
            size_t before = al.size();
            al.erase(unique(al.begin(), al.end()), al.end());
            deduped_edges += (long long)(before - al.size());
        }
        graph.num_edges = 0;
        for (int i = 0; i < n; i++)
            graph.num_edges += (long long)graph.adj_list[i].size();
        graph.num_edges /= 2;
        if (deduped_edges > 0)
            cout << "Deduplicated " << deduped_edges << " duplicate neighbor entries" << endl;
    }

    cout << "Nodes: " << n << ", Edges: " << graph.num_edges << endl;

    // ---- Load Initial Coloring ----
    vector<Node> nodes(n);
    {
        ifstream cfile(color_path);
        if (!cfile.is_open()) { cerr << "ERROR: Cannot open " << color_path << endl; return 1; }
        string line;
        int vertex_id = 0;
        while (getline(cfile, line)) {
            if (line.empty() || line[0] == '%') continue;
            int c = stoi(line);
            if (vertex_id < n) {
                nodes[vertex_id].setColor(c);
                nodes[vertex_id].setPrevColor(c);
            }
            vertex_id++;
        }
        cout << "Loaded colors for " << vertex_id << " vertices" << endl;
    }

    // ---- Verify/Repair Initial State ----
    if (!verifyColoring(graph, nodes, "Initial State"))
        runFullGreedyRepair(graph, nodes);

    int init_chromatic = getChromaticNumber(nodes);
    vector<int> chromatic_history;
    chromatic_history.push_back(init_chromatic);
    cout << "Initial Chromatic Number: " << init_chromatic << endl;

    // ---- Connect to DPU ----
    int dpu_sock = connect_to_dpu(dpu_host, dpu_port);
    if (dpu_sock < 0) return 1;

    // ---- Send Initial Graph to DPU ----
    cout << "[Host] Sending initial graph + colors to DPU..." << endl;
    if (send_initial_graph(dpu_sock, graph, nodes) < 0) {
        cerr << "[Host] Failed to send graph to DPU" << endl;
        close(dpu_sock); return 1;
    }
    cout << "[Host] Graph sent. Ready for batches." << endl;

    // ---- Select Batches ----
    int total_batches = countBatchFiles(batch_folder);
    cout << "Total batch files found: " << total_batches << endl;
    vector<int> selected = selectBatches(total_batches, num_batches);
    cout << "Selected Batches: [";
    for (size_t i = 0; i < selected.size(); i++) {
        if (i > 0) cout << ", ";
        cout << selected[i];
    }
    cout << "]" << endl << endl;

    // ---- Process Batches ----
    vector<BatchMetrics> all_metrics;
    vector<ColorSync> pending_color_syncs;  // color changes from P2+P3 for DPU sync

    auto session_start = chrono::high_resolution_clock::now();

    for (int b = 0; b < (int)selected.size(); b++) {
        int batch_idx = selected[b];
        string batch_path = batch_folder + "/" + to_string(batch_idx) + ".mtx";

        auto t_batch_start = chrono::high_resolution_clock::now();
        BatchMetrics bm;
        bm.id = b;
        bm.filename = to_string(batch_idx) + ".mtx";

        // ===== Pre-Phase: Read + Sort + Graph Update =====
        auto t_pre = chrono::high_resolution_clock::now();

        vector<Edge> batch;
        {
            ifstream bf(batch_path);
            if (!bf.is_open()) {
                cout << "WARNING: " << batch_path << " not found, skipping" << endl;
                continue;
            }
            string line;
            while (getline(bf, line)) {
                if (line.empty()) continue;
                stringstream bss(line);
                int u, v;
                if (bss >> u >> v) {
                    u--; v--;  // MTX 1-indexed
                    if (u >= 0 && u < n && v >= 0 && v < n)
                        batch.emplace_back(u, v, !graph.isAdjacent(u, v));
                }
            }
        }

        // Sort: deletions first, then insertions
        sort(batch.begin(), batch.end(), [](const Edge& a, const Edge& b) {
            if (a.isInsertion != b.isInsertion) return !a.isInsertion;
            return a.source < b.source;
        });

        // Graph Update (topology update BEFORE coloring — correct for GC)
        for (auto& edge : batch) {
            if (edge.isInsertion) graph.addEdge(edge.source, edge.destination);
            else graph.removeEdge(edge.source, edge.destination);
        }

        bm.PrePhase_CPU = chrono::duration<float, milli>(
            chrono::high_resolution_clock::now() - t_pre).count();

        // ===== PHASE 1: ProcessCE on DPU =====
        auto t_p1_start = chrono::high_resolution_clock::now();

        // Prepare batch edges for DPU
        vector<BatchEdge> dpu_edges(batch.size());
        for (size_t i = 0; i < batch.size(); i++) {
            dpu_edges[i].src = (uint32_t)batch[i].source;
            dpu_edges[i].dst = (uint32_t)batch[i].destination;
            dpu_edges[i].is_ins = batch[i].isInsertion ? 1 : 0;
        }

        // Send: num_edges + edges + color_syncs
        uint32_t ne = (uint32_t)dpu_edges.size();
        if (send_all(dpu_sock, &ne, 4) < 0) { cerr << "[Host] Send failed" << endl; break; }
        if (ne > 0 && send_all(dpu_sock, dpu_edges.data(), ne * sizeof(BatchEdge)) < 0) break;

        uint32_t num_cs = (uint32_t)pending_color_syncs.size();
        if (send_all(dpu_sock, &num_cs, 4) < 0) break;
        if (num_cs > 0 && send_all(dpu_sock, pending_color_syncs.data(), num_cs * sizeof(ColorSync)) < 0) break;
        pending_color_syncs.clear();

        // Receive P1 results
        uint32_t num_aff;
        if (recv_all(dpu_sock, &num_aff, 4) < 0) { cerr << "[Host] Recv failed" << endl; break; }
        vector<int> Aff(num_aff);
        if (num_aff > 0 && recv_all(dpu_sock, Aff.data(), num_aff * 4) < 0) break;

        uint32_t num_cc;
        if (recv_all(dpu_sock, &num_cc, 4) < 0) break;
        if (num_cc > 0) {
            vector<ColorChange> cc(num_cc);
            if (recv_all(dpu_sock, cc.data(), num_cc * sizeof(ColorChange)) < 0) break;
            // Apply color changes from DPU to host nodes
            for (auto &c : cc) {
                if (c.node_id < (uint32_t)n) {
                    nodes[c.node_id].setPrevColor(c.old_color);
                    nodes[c.node_id].setColor(c.new_color);
                }
            }
        }

        uint64_t p1_us;
        if (recv_all(dpu_sock, &p1_us, 8) < 0) break;

        auto t_p1_end = chrono::high_resolution_clock::now();
        bm.P1_Total = chrono::duration<float, milli>(t_p1_end - t_p1_start).count();
        bm.P1_DPU = (float)p1_us / 1000.0f;
        bm.P1_Transfer = bm.P1_Total - bm.P1_DPU;

        // ===== Phases 2+3: Loop on CPU =====
        float p2_time = 0, p3_time = 0;
        sort(Aff.begin(), Aff.end());
        Aff.erase(unique(Aff.begin(), Aff.end()), Aff.end());
        int itr = 0;

        // Track color changes during P2+P3 for DPU sync
        vector<pair<uint32_t, int32_t>> p23_changes;

        while (!Aff.empty() && itr < 50) {
            vector<int> S = Aff;
            Aff.clear();

            // Phase 2: CheckConflict (sequential to avoid race conditions)
            auto t2 = chrono::high_resolution_clock::now();
            for (size_t i = 0; i < S.size(); ++i) {
                int v = S[i];
                vector<int> A1 = CheckConflict(graph, nodes, v);
                for (int a : A1)
                    p23_changes.push_back({(uint32_t)a, (int32_t)nodes[a].getColor()});
                Aff.insert(Aff.end(), A1.begin(), A1.end());
            }
            p2_time += chrono::duration<float, milli>(
                chrono::high_resolution_clock::now() - t2).count();

            // Phase 3: UpdateNeighbors (sequential)
            auto t3 = chrono::high_resolution_clock::now();
            {
                vector<int> next_aff;
                for (size_t i = 0; i < Aff.size(); ++i) {
                    int v = Aff[i];
                    vector<int> A2 = UpdateNeighbors(graph, nodes, v);
                    for (int a : A2)
                        p23_changes.push_back({(uint32_t)a, (int32_t)nodes[a].getColor()});
                    next_aff.insert(next_aff.end(), A2.begin(), A2.end());
                }
                Aff = next_aff;
            }
            p3_time += chrono::duration<float, milli>(
                chrono::high_resolution_clock::now() - t3).count();

            sort(Aff.begin(), Aff.end());
            Aff.erase(unique(Aff.begin(), Aff.end()), Aff.end());
            itr++;
        }

        bm.CheckConflict_CPU_P2 = p2_time;
        bm.UpdateNeighbors_CPU_P3 = p3_time;

        // Build pending color syncs for next batch (deduplicate, keep latest)
        // Use a map to keep only the final color per node
        {
            vector<uint32_t> changed_nodes;
            for (auto &[nid, _] : p23_changes)
                changed_nodes.push_back(nid);
            sort(changed_nodes.begin(), changed_nodes.end());
            changed_nodes.erase(unique(changed_nodes.begin(), changed_nodes.end()), changed_nodes.end());
            for (uint32_t nid : changed_nodes)
                pending_color_syncs.push_back({nid, (int32_t)nodes[nid].getColor()});
        }

        // ===== Post-Phase: Verify =====
        auto t_verify = chrono::high_resolution_clock::now();
        bool valid = verifyColoring(graph, nodes, "Batch " + to_string(b));
        bm.Verify_CPU_post = chrono::duration<float, milli>(
            chrono::high_resolution_clock::now() - t_verify).count();

        bm.time_total_batch = chrono::duration<float, milli>(
            chrono::high_resolution_clock::now() - t_batch_start).count();
        bm.iterations = itr;
        bm.chromatic_num = getChromaticNumber(nodes);

        chromatic_history.push_back(bm.chromatic_num);
        all_metrics.push_back(bm);

        cout << fixed << setprecision(3)
             << "Batch " << b << " (file: " << bm.filename << ")"
             << " | Chrom: " << bm.chromatic_num
             << " | Pre: " << bm.PrePhase_CPU << "ms"
             << " | P1_DPU: " << bm.P1_DPU << "ms"
             << " | P1_Xfer: " << bm.P1_Transfer << "ms"
             << " | P2: " << bm.CheckConflict_CPU_P2 << "ms"
             << " | P3: " << bm.UpdateNeighbors_CPU_P3 << "ms"
             << " | Verify: " << bm.Verify_CPU_post << "ms"
             << " | Iter: " << bm.iterations
             << " | Total: " << bm.time_total_batch << "ms"
             << (valid ? "" : " [INVALID]")
             << endl;
    }

    // ---- Send shutdown sentinel ----
    uint32_t sentinel = SHUTDOWN_SENTINEL;
    send_all(dpu_sock, &sentinel, 4);
    close(dpu_sock);
    cout << "[Host] DPU session closed." << endl;

    auto session_end = chrono::high_resolution_clock::now();
    float session_time_ms = chrono::duration<float, milli>(session_end - session_start).count();

    // ============ SUMMARY ============
    int cnt = (int)all_metrics.size();
    if (cnt == 0) {
        cout << "No batches processed." << endl;
        return 0;
    }

    float avg_pre = 0, avg_p1_dpu = 0, avg_p1_xfer = 0, avg_p1_total = 0;
    float avg_p2 = 0, avg_p3 = 0, avg_verify = 0, avg_total = 0;
    int tot_iters = 0;
    for (const auto& m : all_metrics) {
        avg_pre += m.PrePhase_CPU;
        avg_p1_dpu += m.P1_DPU;
        avg_p1_xfer += m.P1_Transfer;
        avg_p1_total += m.P1_Total;
        avg_p2 += m.CheckConflict_CPU_P2;
        avg_p3 += m.UpdateNeighbors_CPU_P3;
        avg_verify += m.Verify_CPU_post;
        avg_total += m.time_total_batch;
        tot_iters += m.iterations;
    }

    cout << endl;
    cout << "==================== SUMMARY ====================" << endl;
    cout << "Batches Processed:       " << cnt << endl;
    cout << "Initial Chromatic Num:   " << chromatic_history.front() << endl;
    cout << "Final Chromatic Num:     " << chromatic_history.back() << endl;
    cout << "Avg Iterations/Batch:    " << fixed << setprecision(1) << (float)tot_iters / cnt << endl;
    cout << "Session Time:            " << fixed << setprecision(3) << session_time_ms << " ms" << endl;
    cout << "Avg Latency:             " << session_time_ms / cnt << " ms" << endl;
    cout << endl;
    cout << "--- Average Timings (ms) per Batch ---" << endl;
    cout << "PrePhase_CPU             " << avg_pre / cnt << endl;
    cout << "P1_DPU                   " << avg_p1_dpu / cnt << endl;
    cout << "P1_Transfer              " << avg_p1_xfer / cnt << endl;
    cout << "P1_Total                 " << avg_p1_total / cnt << endl;
    cout << "CheckConflict_CPU_P2     " << avg_p2 / cnt << endl;
    cout << "UpdateNeighbors_CPU_P3   " << avg_p3 / cnt << endl;
    cout << "Verify_CPU_post          " << avg_verify / cnt << endl;
    cout << "Total Batch (avg):       " << avg_total / cnt << endl;
    cout << "==================================================" << endl;

    // ---- Write CSV ----
    string csv_path = "results.csv";
    bool csv_exists = ifstream(csv_path).good();
    ofstream csv(csv_path, ios::app);
    if (csv.is_open()) {
        if (!csv_exists) {
            csv << "Dataset,BatchRatio,BatchID,BatchFile,"
                << "PrePhase_CPU,P1_DPU,P1_Transfer,P1_Total,"
                << "CheckConflict_CPU_P2,UpdateNeighbors_CPU_P3,"
                << "Verify_CPU_post,TotalBatch_ms,Iterations,ChromaticNum,"
                << "SessionTime_ms,AvgLatency_ms" << endl;
        }

        string dataset_name = "unknown", batch_ratio = "unknown";
        {
            string bf = batch_folder;
            if (!bf.empty() && bf.back() == '/') bf.pop_back();
            size_t pos = bf.rfind('/');
            if (pos != string::npos) {
                batch_ratio = bf.substr(pos + 1);
                bf = bf.substr(0, pos);
                pos = bf.rfind('/');
                if (pos != string::npos) {
                    bf = bf.substr(0, pos);
                    pos = bf.rfind('/');
                    if (pos != string::npos)
                        dataset_name = bf.substr(pos + 1);
                }
            }
        }

        for (const auto& m : all_metrics) {
            csv << dataset_name << "," << batch_ratio << ","
                << m.id << "," << m.filename << ","
                << fixed << setprecision(3)
                << m.PrePhase_CPU << "," << m.P1_DPU << ","
                << m.P1_Transfer << "," << m.P1_Total << ","
                << m.CheckConflict_CPU_P2 << "," << m.UpdateNeighbors_CPU_P3 << ","
                << m.Verify_CPU_post << "," << m.time_total_batch << ","
                << m.iterations << "," << m.chromatic_num << ","
                << session_time_ms << "," << session_time_ms / cnt << endl;
        }
        csv.close();
        cout << "CSV results appended to: " << csv_path << endl;
    }

    return 0;
}
