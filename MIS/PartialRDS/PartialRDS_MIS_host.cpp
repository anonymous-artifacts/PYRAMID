// PartialRDS_MIS_host.cpp — Dynamic MIS with Phase 1 offloaded to DPU
// Phase 1 (BFS): DPU via TCP
// Phase 2 (Clustering): CPU
// Phase 3 (MIS Computation + Topology Update): CPU
//
// Compile: g++ -O3 -fopenmp -std=c++17 PartialRDS_MIS_host.cpp -o PartialRDS_MIS_host
// Run:     ./PartialRDS_MIS_host <MTX> <MIS_File> <BatchesFolder> <NumBatches> <Threads> <DPU_Host> [DPU_Port]

#include <omp.h>
#include <iostream>
#include <vector>
#include <fstream>
#include <sstream>
#include <unordered_set>
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

int n_threads = 32;
#define SHUTDOWN_SENTINEL 0xFFFFFFFF

// ============================================================================
//                          DATA STRUCTURES
// ============================================================================

struct RawEdge { uint32_t src, dst; };
struct EdgeMutation { uint32_t src, dst, action; };
struct DeltaEntry { uint32_t node_id, new_val; };

struct GraphSyncPayload {
    vector<EdgeMutation> edge_mutations;
    vector<DeltaEntry> membership_changes;
};

struct DpuBfsResponse {
    vector<uint32_t> is_ins;
    vector<uint32_t> aff_counts;
    vector<uint32_t> affected;
    uint64_t bfs_us;
    bool ok;
};

struct Node {
    int label;
    bool membership;
    Node(int l = 0, bool m = false) : label(l), membership(m) {}
};

// Host graph with sorted adjacency for binary search
struct HostGraph {
    uint32_t nv;
    vector<vector<uint32_t>> adj;

    void build_from_csr(uint32_t n, const vector<uint32_t> &offsets,
                         const vector<uint32_t> &nbrs) {
        nv = n;
        adj.resize(nv);
        for (uint32_t u = 0; u < nv; u++)
            adj[u].assign(nbrs.begin() + offsets[u], nbrs.begin() + offsets[u + 1]);
    }

    bool is_adjacent(uint32_t u, uint32_t v) const {
        if (u >= nv) return false;
        return binary_search(adj[u].begin(), adj[u].end(), v);
    }

    void insert_edge(uint32_t u, uint32_t v) {
        auto ins = [](vector<uint32_t> &a, uint32_t val) {
            auto it = lower_bound(a.begin(), a.end(), val);
            if (it == a.end() || *it != val) a.insert(it, val);
        };
        if (u < nv) ins(adj[u], v);
        if (v < nv) ins(adj[v], u);
    }

    void remove_edge(uint32_t u, uint32_t v) {
        auto rem = [](vector<uint32_t> &a, uint32_t val) {
            auto it = lower_bound(a.begin(), a.end(), val);
            if (it != a.end() && *it == val) a.erase(it);
        };
        if (u < nv) rem(adj[u], v);
        if (v < nv) rem(adj[v], u);
    }

    const vector<uint32_t>& neighbors(uint32_t u) const { return adj[u]; }
};

struct BatchMetrics {
    int id;
    string filename;
    float PrePhase_CPU;
    float P1_DPU;        // DPU-side BFS time
    float P1_Transfer;   // Network transfer time
    float P1_Total;      // Total P1 wall time (transfer + DPU)
    float Clustering_CPU_P2;
    float MISComp_CPU_P3;
    float Verify_CPU_post;
    float time_total_batch;
    int mis_cardinality;
    int num_clusters;
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
        cerr << "[Host] Make sure PartialRDS_MIS_dpu is running on the DPU device." << endl;
        close(sock); return -1;
    }
    // Reset timeout for data transfer
    tv.tv_sec = 0; tv.tv_usec = 0;
    setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
    cout << "[Host] Connected to DPU." << endl;
    return sock;
}

// ============================================================================
//                     PROTOCOL: Send/Receive with DPU
// ============================================================================

static int send_batch_with_sync(int sock, const vector<RawEdge> &batch,
                                 const GraphSyncPayload &sync) {
    uint32_t ne = batch.size();
    if (send_all(sock, &ne, 4) < 0) return -1;
    if (ne > 0 && send_all(sock, batch.data(), ne * sizeof(RawEdge)) < 0) return -1;

    uint32_t num_mut = sync.edge_mutations.size();
    if (send_all(sock, &num_mut, 4) < 0) return -1;
    if (num_mut > 0 && send_all(sock, sync.edge_mutations.data(), num_mut * sizeof(EdgeMutation)) < 0) return -1;

    uint32_t num_mem = sync.membership_changes.size();
    if (send_all(sock, &num_mem, 4) < 0) return -1;
    if (num_mem > 0 && send_all(sock, sync.membership_changes.data(), num_mem * sizeof(DeltaEntry)) < 0) return -1;

    return 0;
}

static DpuBfsResponse recv_bfs_results(int sock) {
    DpuBfsResponse resp;
    resp.ok = true;
    resp.bfs_us = 0;

    uint32_t num_edges;
    if (recv_all(sock, &num_edges, 4) < 0) { resp.ok = false; return resp; }

    if (num_edges == 0) {
        uint32_t total_aff;
        if (recv_all(sock, &total_aff, 4) < 0) { resp.ok = false; return resp; }
        if (recv_all(sock, &resp.bfs_us, 8) < 0) { resp.ok = false; return resp; }
        return resp;
    }

    resp.is_ins.resize(num_edges);
    resp.aff_counts.resize(num_edges);
    if (recv_all(sock, resp.is_ins.data(), num_edges * 4) < 0) { resp.ok = false; return resp; }
    if (recv_all(sock, resp.aff_counts.data(), num_edges * 4) < 0) { resp.ok = false; return resp; }

    uint32_t total_aff;
    if (recv_all(sock, &total_aff, 4) < 0) { resp.ok = false; return resp; }

    if (total_aff > 0) {
        resp.affected.resize(total_aff);
        if (recv_all(sock, resp.affected.data(), total_aff * 4) < 0) { resp.ok = false; return resp; }
    }

    if (recv_all(sock, &resp.bfs_us, 8) < 0) { resp.ok = false; return resp; }
    return resp;
}

// ============================================================================
//                   SEND INITIAL GRAPH TO DPU (CSR)
// ============================================================================

static int send_initial_graph(int sock, uint32_t n, const vector<uint32_t> &offsets,
                               const vector<uint32_t> &nbrs, const vector<uint8_t> &membership) {
    uint32_t ne = (uint32_t)nbrs.size();
    if (send_all(sock, &n, 4) < 0) return -1;
    if (send_all(sock, &ne, 4) < 0) return -1;
    if (send_all(sock, offsets.data(), (size_t)(n + 1) * 4) < 0) return -1;
    if (ne > 0 && send_all(sock, nbrs.data(), (size_t)ne * 4) < 0) return -1;
    if (send_all(sock, membership.data(), n) < 0) return -1;
    return 0;
}

// ============================================================================
//                  CPU-SIDE MIS UPDATE HELPERS
// ============================================================================

static void cpu_handle_insertion(uint32_t u, uint32_t v,
                                  const HostGraph &graph,
                                  vector<uint8_t> &membership,
                                  vector<uint32_t> &changed) {
    if (membership[u] && membership[v]) {
        uint32_t rem = min(u, v);
        membership[rem] = 0;
        changed.push_back(rem);
        for (uint32_t nb : graph.neighbors(rem)) {
            if (!membership[nb]) {
                bool ok = true;
                for (uint32_t nb2 : graph.neighbors(nb))
                    if (membership[nb2]) { ok = false; break; }
                if (ok) { membership[nb] = 1; changed.push_back(nb); }
            }
        }
    }
}

static void cpu_handle_deletion(uint32_t u, uint32_t v,
                                 const HostGraph &graph,
                                 vector<uint8_t> &membership,
                                 vector<uint32_t> &changed) {
    if (membership[u] && !membership[v]) {
        bool ok = true;
        for (uint32_t nb : graph.neighbors(v))
            if (nb != u && membership[nb]) { ok = false; break; }
        if (ok) { membership[v] = 1; changed.push_back(v); }
    } else if (!membership[u] && membership[v]) {
        bool ok = true;
        for (uint32_t nb : graph.neighbors(u))
            if (nb != v && membership[nb]) { ok = false; break; }
        if (ok) { membership[u] = 1; changed.push_back(u); }
    }
}

// ============================================================================
//               PHASE 2: CLUSTERING FROM FLAT BFS BUFFER
// ============================================================================

struct Cluster { vector<int> edge_indices; };

static vector<Cluster> cluster_from_bfs(const DpuBfsResponse &bfs, uint32_t nv) {
    uint32_t ne = bfs.is_ins.size();

    // Rebuild aff_starts from aff_counts (prefix sum)
    vector<uint32_t> aff_starts(ne + 1, 0);
    for (uint32_t i = 0; i < ne; i++)
        aff_starts[i + 1] = aff_starts[i] + bfs.aff_counts[i];

    // Clustering: merge overlapping affected areas
    vector<int32_t> node_to_clust(nv, -1);
    vector<Cluster> clusters;

    for (uint32_t i = 0; i < ne; i++) {
        uint32_t start = aff_starts[i];
        uint32_t count = bfs.aff_counts[i];

        // Find overlapping clusters
        int overlap[128];
        int n_ov = 0;
        for (uint32_t j = 0; j < count; j++) {
            uint32_t node = bfs.affected[start + j];
            if (node < nv) {
                int32_t c = node_to_clust[node];
                if (c != -1) {
                    bool found = false;
                    for (int k = 0; k < n_ov; k++) if (overlap[k] == c) { found = true; break; }
                    if (!found && n_ov < 128) overlap[n_ov++] = c;
                }
            }
        }

        int target;
        if (n_ov == 0) {
            target = (int)clusters.size();
            clusters.push_back({});
        } else {
            target = overlap[0];
            for (int k = 1; k < n_ov; k++)
                if ((int)clusters[overlap[k]].edge_indices.size() >
                    (int)clusters[target].edge_indices.size())
                    target = overlap[k];
            for (int k = 0; k < n_ov; k++) {
                int oc = overlap[k];
                if (oc != target) {
                    for (int ei : clusters[oc].edge_indices)
                        clusters[target].edge_indices.push_back(ei);
                    clusters[oc].edge_indices.clear();
                }
            }
        }

        clusters[target].edge_indices.push_back((int)i);

        for (uint32_t j = 0; j < count; j++) {
            uint32_t node = bfs.affected[start + j];
            if (node < nv) node_to_clust[node] = target;
        }
    }

    // Remove empty clusters
    clusters.erase(
        remove_if(clusters.begin(), clusters.end(),
                  [](const Cluster &c) { return c.edge_indices.empty(); }),
        clusters.end());

    return clusters;
}

// ============================================================================
//               PHASE 3: MIS COMPUTATION + TOPOLOGY UPDATE
// ============================================================================

static GraphSyncPayload mis_update_per_cluster(
        const vector<Cluster> &clusters,
        const DpuBfsResponse &bfs,
        const vector<RawEdge> &batch_edges,
        HostGraph &graph,
        vector<uint8_t> &membership) {

    GraphSyncPayload sync;
    vector<uint32_t> changed;
    changed.reserve(1024);

    for (auto &cluster : clusters) {
        // Sort edge indices: deletions first, then insertions (matches CPU_Only)
        vector<int> sorted_indices = cluster.edge_indices;
        sort(sorted_indices.begin(), sorted_indices.end(),
             [&bfs](int a, int b) {
                 return bfs.is_ins[a] < bfs.is_ins[b];  // 0 (del) before 1 (ins)
             });

        for (int ei : sorted_indices) {
            uint32_t src = batch_edges[ei].src;
            uint32_t dst = batch_edges[ei].dst;
            if (src >= graph.nv || dst >= graph.nv) continue;

            if (bfs.is_ins[ei]) {
                cpu_handle_insertion(src, dst, graph, membership, changed);
                graph.insert_edge(src, dst);
                sync.edge_mutations.push_back({src, dst, 1});
            } else {
                cpu_handle_deletion(src, dst, graph, membership, changed);
                graph.remove_edge(src, dst);
                sync.edge_mutations.push_back({src, dst, 0});
            }
        }
    }

    // Deduplicate changed nodes → membership_changes
    sort(changed.begin(), changed.end());
    changed.erase(unique(changed.begin(), changed.end()), changed.end());
    for (uint32_t node : changed)
        sync.membership_changes.push_back({node, (uint32_t)membership[node]});

    return sync;
}

// ============================================================================
//                     MIS VERIFICATION
// ============================================================================

static bool verifyMIS(const HostGraph &graph, const vector<uint8_t> &membership) {
    long long viol_ind = 0, viol_max = 0;

    #pragma omp parallel for num_threads(n_threads) reduction(+:viol_ind, viol_max)
    for (uint32_t u = 0; u < graph.nv; u++) {
        if (membership[u]) {
            for (uint32_t v : graph.adj[u])
                if (u < v && membership[v]) viol_ind++;
        } else {
            if (graph.adj[u].empty()) continue;
            bool has = false;
            for (uint32_t v : graph.adj[u])
                if (membership[v]) { has = true; break; }
            if (!has) viol_max++;
        }
    }

    if (viol_ind > 0 || viol_max > 0) {
        cout << "[FAILED] " << viol_ind << " independence, "
             << viol_max << " maximality violations" << endl;
        return false;
    }
    return true;
}

static int countMIS(const vector<uint8_t> &membership) {
    int count = 0;
    for (auto m : membership) if (m) count++;
    return count;
}

// ============================================================================
//                     GRAPH I/O
// ============================================================================

// Read MTX and build CSR (sorted adjacency)
static void read_graph_to_csr(const string &filename, uint32_t &n,
                                vector<uint32_t> &offsets, vector<uint32_t> &nbrs,
                                vector<vector<uint32_t>> &adj_list) {
    ifstream file(filename);
    if (!file.is_open()) {
        cerr << "Cannot open: " << filename << endl;
        return;
    }

    string line;
    while (getline(file, line)) {
        if (line.empty() || line[0] == '%') continue;
        break;
    }

    long long nodes_ll, cols_ll, edges_ll;
    stringstream ss(line);
    ss >> nodes_ll >> cols_ll >> edges_ll;
    n = (uint32_t)nodes_ll;

    adj_list.resize(n);
    while (getline(file, line)) {
        if (line.empty()) continue;
        uint32_t u, v;
        stringstream ss2(line);
        ss2 >> u >> v;
        if (u < n && v < n) {
            adj_list[u].push_back(v);
            adj_list[v].push_back(u);
        }
    }

    // Sort + deduplicate adjacency lists (matches final_set build_sorted_adj)
    for (uint32_t i = 0; i < n; i++) {
        sort(adj_list[i].begin(), adj_list[i].end());
        adj_list[i].erase(unique(adj_list[i].begin(), adj_list[i].end()), adj_list[i].end());
    }

    // Build CSR
    offsets.resize(n + 1, 0);
    for (uint32_t i = 0; i < n; i++)
        offsets[i + 1] = offsets[i] + (uint32_t)adj_list[i].size();

    uint32_t total_ne = offsets[n];
    nbrs.resize(total_ne);
    for (uint32_t i = 0; i < n; i++)
        for (uint32_t j = 0; j < (uint32_t)adj_list[i].size(); j++)
            nbrs[offsets[i] + j] = adj_list[i][j];

    cout << "Graph: " << n << " nodes, " << total_ne << " CSR edges" << endl;
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
             << " <MTX> <MIS_File> <BatchesFolder> <NumBatches> <Threads> <DPU_Host> [DPU_Port]" << endl;
        return 1;
    }

    string mtx_path = argv[1];
    string mis_path = argv[2];
    string batch_folder = argv[3];
    int num_batches = stoi(argv[4]);
    n_threads = stoi(argv[5]);
    const char *dpu_host = argv[6];
    int dpu_port = (argc > 7) ? stoi(argv[7]) : 5000;

    cout << "==================== PartialRDS_MIS ====================" << endl;
    cout << "MTX:      " << mtx_path << endl;
    cout << "MIS:      " << mis_path << endl;
    cout << "Batches:  " << batch_folder << endl;
    cout << "NumBatch: " << num_batches << endl;
    cout << "Threads:  " << n_threads << endl;
    cout << "DPU:      " << dpu_host << ":" << dpu_port << endl;
    cout << "========================================================" << endl;

    // ---- Load Graph + Build CSR ----
    uint32_t num_nodes;
    vector<uint32_t> csr_offsets, csr_nbrs;
    vector<vector<uint32_t>> adj_list;
    read_graph_to_csr(mtx_path, num_nodes, csr_offsets, csr_nbrs, adj_list);

    // ---- Build HostGraph ----
    HostGraph graph;
    graph.nv = num_nodes;
    graph.adj = move(adj_list);

    // ---- Initialize Membership ----
    vector<uint8_t> membership(num_nodes, 0);
    int initial_card = 0;
    {
        ifstream mis_file(mis_path);
        if (!mis_file.is_open()) {
            cerr << "ERROR: Cannot open MIS file: " << mis_path << endl;
            return 1;
        }
        string line;
        while (getline(mis_file, line)) {
            if (line.empty()) continue;
            int v = stoi(line);
            if (v + 1 < (int)num_nodes) {
                membership[v + 1] = 1;
                initial_card++;
            }
        }
    }
    cout << "Initial MIS Cardinality: " << initial_card << endl;

    // ---- Connect to DPU ----
    int dpu_sock = connect_to_dpu(dpu_host, dpu_port);
    if (dpu_sock < 0) return 1;

    // ---- Send Initial Graph to DPU ----
    cout << "[Host] Sending initial graph to DPU (" << num_nodes << " nodes, "
         << csr_nbrs.size() << " CSR edges)..." << endl;
    if (send_initial_graph(dpu_sock, num_nodes, csr_offsets, csr_nbrs, membership) < 0) {
        cerr << "[Host] Failed to send graph to DPU" << endl;
        close(dpu_sock); return 1;
    }
    cout << "[Host] Graph sent. Ready for batches." << endl;

    // Free CSR (no longer needed, graph is in HostGraph and on DPU)
    csr_offsets.clear(); csr_offsets.shrink_to_fit();
    csr_nbrs.clear(); csr_nbrs.shrink_to_fit();

    // ---- Select Batches ----
    int total_batches = countBatchFiles(batch_folder);
    cout << "Total batch files found: " << total_batches << endl;
    vector<int> selected = selectBatches(total_batches, num_batches);
    cout << "Selected Batches: [";
    for (size_t i = 0; i < selected.size(); i++) {
        if (i > 0) cout << ", ";
        cout << selected[i];
    }
    cout << "]" << endl;

    // ---- Process Batches ----
    vector<BatchMetrics> all_metrics;
    GraphSyncPayload pending_sync;  // sync from previous batch

    auto session_start = chrono::high_resolution_clock::now();

    for (int b = 0; b < (int)selected.size(); b++) {
        int batch_idx = selected[b];
        string batch_path = batch_folder + "/" + to_string(batch_idx) + ".mtx";

        auto t_batch_start = chrono::high_resolution_clock::now();
        BatchMetrics bm;
        bm.id = b;
        bm.filename = to_string(batch_idx) + ".mtx";

        // ============ PRE-PHASE: Read batch file ============
        auto t_pre = chrono::high_resolution_clock::now();

        vector<RawEdge> batch_edges;
        {
            ifstream bf(batch_path);
            if (!bf.is_open()) {
                cout << "WARNING: " << batch_path << " not found, skipping" << endl;
                continue;
            }
            string line;
            while (getline(bf, line)) {
                if (line.empty()) continue;
                stringstream ss(line);
                uint32_t src, dst;
                if (ss >> src >> dst) {
                    batch_edges.push_back({src, dst});
                }
            }
        }

        bm.PrePhase_CPU = chrono::duration<float, milli>(
            chrono::high_resolution_clock::now() - t_pre).count();

        // ============ PHASE 1: BFS on DPU ============
        // Send batch edges + sync from previous batch → receive BFS results
        auto t_p1_start = chrono::high_resolution_clock::now();

        if (send_batch_with_sync(dpu_sock, batch_edges, pending_sync) < 0) {
            cerr << "[Host] Failed to send batch to DPU" << endl;
            break;
        }
        pending_sync = {};  // Clear after sending

        DpuBfsResponse bfs = recv_bfs_results(dpu_sock);
        if (!bfs.ok) {
            cerr << "[Host] Failed to receive BFS results from DPU" << endl;
            break;
        }

        auto t_p1_end = chrono::high_resolution_clock::now();
        bm.P1_Total = chrono::duration<float, milli>(t_p1_end - t_p1_start).count();
        bm.P1_DPU = (float)bfs.bfs_us / 1000.0f;  // us → ms
        bm.P1_Transfer = bm.P1_Total - bm.P1_DPU;

        // ============ PHASE 2: Clustering on CPU ============
        auto t_p2 = chrono::high_resolution_clock::now();
        vector<Cluster> clusters = cluster_from_bfs(bfs, graph.nv);
        bm.Clustering_CPU_P2 = chrono::duration<float, milli>(
            chrono::high_resolution_clock::now() - t_p2).count();
        bm.num_clusters = (int)clusters.size();

        // ============ PHASE 3: MIS Computation + Topology Update ============
        auto t_p3 = chrono::high_resolution_clock::now();
        pending_sync = mis_update_per_cluster(clusters, bfs, batch_edges, graph, membership);
        bm.MISComp_CPU_P3 = chrono::duration<float, milli>(
            chrono::high_resolution_clock::now() - t_p3).count();

        // ============ POST-PHASE: Verification ============
        auto t_post = chrono::high_resolution_clock::now();
        bool valid = verifyMIS(graph, membership);
        bm.Verify_CPU_post = chrono::duration<float, milli>(
            chrono::high_resolution_clock::now() - t_post).count();

        bm.mis_cardinality = countMIS(membership);
        bm.time_total_batch = chrono::duration<float, milli>(
            chrono::high_resolution_clock::now() - t_batch_start).count();

        all_metrics.push_back(bm);

        cout << "Batch " << b << " (file: " << bm.filename << ")"
             << " | Card: " << bm.mis_cardinality
             << " | Clusters: " << bm.num_clusters
             << " | Pre: " << fixed << setprecision(3) << bm.PrePhase_CPU << "ms"
             << " | P1_DPU: " << bm.P1_DPU << "ms"
             << " | P1_Xfer: " << bm.P1_Transfer << "ms"
             << " | Clust: " << bm.Clustering_CPU_P2 << "ms"
             << " | MIS: " << bm.MISComp_CPU_P3 << "ms"
             << " | Verify: " << bm.Verify_CPU_post << "ms"
             << " | Total: " << bm.time_total_batch << "ms"
             << (valid ? "" : " [INVALID]")
             << endl;
    }

    // ---- Send shutdown sentinel to DPU ----
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
    float avg_clust = 0, avg_mis = 0, avg_verify = 0, avg_total = 0;
    for (const auto& m : all_metrics) {
        avg_pre += m.PrePhase_CPU;
        avg_p1_dpu += m.P1_DPU;
        avg_p1_xfer += m.P1_Transfer;
        avg_p1_total += m.P1_Total;
        avg_clust += m.Clustering_CPU_P2;
        avg_mis += m.MISComp_CPU_P3;
        avg_verify += m.Verify_CPU_post;
        avg_total += m.time_total_batch;
    }

    cout << endl;
    cout << "==================== SUMMARY ====================" << endl;
    cout << "Batches Processed:       " << cnt << endl;
    cout << "Initial MIS Cardinality: " << initial_card << endl;
    cout << "Final MIS Cardinality:   " << all_metrics.back().mis_cardinality << endl;
    cout << "Session Time:            " << fixed << setprecision(3) << session_time_ms << " ms" << endl;
    cout << "Avg Latency:             " << session_time_ms / cnt << " ms" << endl;
    cout << endl;
    cout << "--- Average Timings (ms) per Batch ---" << endl;
    cout << "PrePhase_CPU             " << avg_pre / cnt << endl;
    cout << "P1_DPU                   " << avg_p1_dpu / cnt << endl;
    cout << "P1_Transfer              " << avg_p1_xfer / cnt << endl;
    cout << "P1_Total                 " << avg_p1_total / cnt << endl;
    cout << "Clustering_CPU_P2        " << avg_clust / cnt << endl;
    cout << "MISComp_CPU_P3           " << avg_mis / cnt << endl;
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
                << "Clustering_CPU_P2,MISComp_CPU_P3,"
                << "Verify_CPU_post,TotalBatch_ms,MIS_Cardinality,NumClusters,"
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
                << m.Clustering_CPU_P2 << "," << m.MISComp_CPU_P3 << ","
                << m.Verify_CPU_post << "," << m.time_total_batch << ","
                << m.mis_cardinality << "," << m.num_clusters << ","
                << session_time_ms << "," << session_time_ms / cnt << endl;
        }
        csv.close();
        cout << "CSV results appended to: " << csv_path << endl;
    }

    return 0;
}
