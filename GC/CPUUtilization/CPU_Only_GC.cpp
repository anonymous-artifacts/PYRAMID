// CPU_Only_GC.cpp — Dynamic Graph Coloring with 3-Phase Pipeline
// Phase 1: ProcessCE (Deletion optimization + Insertion conflict detection)
// Phase 2: CheckConflict (Conflict resolution)
// Phase 3: UpdateNeighbors (Color improvement using freed colors)
//
// Compile: g++ -O2 -fopenmp -std=c++17 CPU_Only_GC.cpp -o CPU_Only_GC
// Run:     ./CPU_Only_GC <MTX> <Colors> <BatchesFolder> <NumBatches> <Threads>

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

using namespace std;

#define MAX_COLORS 1024

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

struct BatchMetrics {
    int id;
    string filename;
    float PrePhase_CPU;
    float ProcessCE_CPU_P1;
    float CheckConflict_CPU_P2;
    float UpdateNeighbors_CPU_P3;
    float Verify_CPU_post;
    int iterations;
    int chromatic_num;
    float time_total_batch;
    float PrePhase_Util;
    float P1_Util;
    float P2_Util;
    float P3_Util;
    float Verify_Util;
};

// ============================================================================
//                          UTILITY FUNCTIONS
// ============================================================================

int getChromaticNumber(const vector<Node>& nodes) {
    int max_c = -1;
    for (const auto& n : nodes) if (n.getColor() > max_c) max_c = n.getColor();
    return max_c + 1;
}

bool verifyColoring(const Graph& graph, const vector<Node>& nodes, const string& phase,
                    double& out_cpu_time_ms) {
    long long conflicts = 0;
    int uncolored = 0;
    double thread_cpu = 0.0;

    #pragma omp parallel for num_threads(n_threads) reduction(+:conflicts, uncolored, thread_cpu)
    for (int u = 0; u < graph.num_nodes; u++) {
        double t0 = omp_get_wtime();
        int cu = nodes[u].getColor();
        if (cu == -1) { uncolored++; }
        else {
            for (int v : graph.adj_list[u]) {
                if (u < v && nodes[v].getColor() == cu) conflicts++;
            }
        }
        thread_cpu += (omp_get_wtime() - t0) * 1000.0;
    }

    out_cpu_time_ms = thread_cpu;

    if (conflicts > 0 || uncolored > 0) {
        cout << "[FAILED] " << phase << ": " << conflicts << " conflicts, " << uncolored << " uncolored." << endl;
        return false;
    }
    return true;
}

// Overload for initial verify (no CPU time tracking needed)
bool verifyColoring(const Graph& graph, const vector<Node>& nodes, const string& phase) {
    double dummy;
    return verifyColoring(graph, nodes, phase, dummy);
}

int getSmallestAvailableColor_Naive(int u, const Graph& graph, const vector<Node>& nodes) {
    for (int c = 0; c < MAX_COLORS; c++) {
        bool is_used = false;
        for (int v : graph.adj_list[u]) {
            if (nodes[v].getColor() == c) {
                is_used = true;
                break;
            }
        }
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
            for (int nbr : graph.adj_list[u]) {
                if (nodes[nbr].getColor() == freed_color) {
                    in_SC = true;
                    break;
                }
            }
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
        else if (i > count + 5) break; // stop searching after gap
    }
    return count;
}

vector<int> selectBatches(int total, int select, unsigned seed = 42) {
    vector<int> indices(total);
    iota(indices.begin(), indices.end(), 1); // 1..total
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
    if (argc < 6) {
        cerr << "Usage: " << argv[0] << " <MTX> <Colors> <BatchesFolder> <NumBatches> <Threads>" << endl;
        return 1;
    }

    string mtx_path = argv[1];
    string color_path = argv[2];
    string batch_folder = argv[3];
    int num_batches = stoi(argv[4]);
    n_threads = stoi(argv[5]);

    cout << "==================== CPU_Only_GC ====================" << endl;
    cout << "MTX:     " << mtx_path << endl;
    cout << "Colors:  " << color_path << endl;
    cout << "Batches: " << batch_folder << endl;
    cout << "NumBatch:" << num_batches << endl;
    cout << "Threads: " << n_threads << endl;
    cout << "=====================================================" << endl;

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
                u--; v--; // MTX is 1-indexed
                if (u >= 0 && u < n && v >= 0 && v < n)
                    graph.addEdge(u, v);
            }
        }
    }
    int n = graph.num_nodes;
    cout << "Nodes: " << n << ", Edges: " << graph.num_edges << endl;

    // ---- Load Initial Coloring ----
    // Format: one color per line, line number = vertex index (0-based)
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

        // Read batch file
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
                    u--; v--; // MTX 1-indexed
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
        auto mid = find_if(batch.begin(), batch.end(), [](const Edge& e) { return e.isInsertion; });

        // Graph Update (topology update before coloring — correct for GC)
        for (auto& edge : batch) {
            if (edge.isInsertion) graph.addEdge(edge.source, edge.destination);
            else graph.removeEdge(edge.source, edge.destination);
        }

        bm.PrePhase_CPU = chrono::duration<float, milli>(chrono::high_resolution_clock::now() - t_pre).count();
        bm.PrePhase_Util = (1.0f / n_threads) * 100.0f; // sequential

        vector<int> Aff;

        // ===== Phase 1: ProcessCE =====
        auto t_p1 = chrono::high_resolution_clock::now();

        // Deletions
        for (long long i = 0; i < distance(batch.begin(), mid); ++i) {
            int a = batch[i].source, b2 = batch[i].destination;
            int ca = nodes[a].getColor(), cb = nodes[b2].getColor();
            int y = (ca >= cb ? a : b2);
            int z = (y == a ? b2 : a);
            int target_color = nodes[z].getColor();

            bool in_SC = false;
            for (int nbr : graph.adj_list[y]) {
                if (nbr == z) continue;
                if (nodes[nbr].getColor() == target_color) {
                    in_SC = true;
                    break;
                }
            }
            if (!in_SC) {
                nodes[y].setPrevColor(nodes[y].getColor());
                nodes[y].setColor(target_color);
                Aff.push_back(y);
            }
        }

        // Insertions
        for (long long i = distance(batch.begin(), mid); i < (long long)batch.size(); ++i) {
            int a = batch[i].source, b2 = batch[i].destination;
            if (nodes[a].getColor() == nodes[b2].getColor()) {
                int y = (a > b2 ? a : b2);
                nodes[y].setPrevColor(nodes[y].getColor());
                int new_color = getSmallestAvailableColor_Naive(y, graph, nodes);
                nodes[y].setColor(new_color);
                Aff.push_back(y);
            }
        }

        bm.ProcessCE_CPU_P1 = chrono::duration<float, milli>(chrono::high_resolution_clock::now() - t_p1).count();
        bm.P1_Util = (1.0f / n_threads) * 100.0f; // sequential

        // ===== Phases 2+3: Loop =====
        float p2_time = 0, p3_time = 0;
        sort(Aff.begin(), Aff.end());
        Aff.erase(unique(Aff.begin(), Aff.end()), Aff.end());
        int itr = 0;

        while (!Aff.empty() && itr < 50) {
            vector<int> S = Aff;
            Aff.clear();

            // Phase 2: CheckConflict
            auto t2 = chrono::high_resolution_clock::now();
            for (size_t i = 0; i < S.size(); ++i) {
                int v = S[i];
                vector<int> A1 = CheckConflict(graph, nodes, v);
                Aff.insert(Aff.end(), A1.begin(), A1.end());
            }
            p2_time += chrono::duration<float, milli>(chrono::high_resolution_clock::now() - t2).count();

            // Phase 3: UpdateNeighbors
            auto t3 = chrono::high_resolution_clock::now();
            {
                vector<int> next_aff;
                for (size_t i = 0; i < Aff.size(); ++i) {
                    int v = Aff[i];
                    vector<int> A2 = UpdateNeighbors(graph, nodes, v);
                    next_aff.insert(next_aff.end(), A2.begin(), A2.end());
                }
                Aff = next_aff;
            }
            p3_time += chrono::duration<float, milli>(chrono::high_resolution_clock::now() - t3).count();

            sort(Aff.begin(), Aff.end());
            Aff.erase(unique(Aff.begin(), Aff.end()), Aff.end());
            itr++;
        }

        bm.CheckConflict_CPU_P2 = p2_time;
        bm.P2_Util = (1.0f / n_threads) * 100.0f; // sequential
        bm.UpdateNeighbors_CPU_P3 = p3_time;
        bm.P3_Util = (1.0f / n_threads) * 100.0f; // sequential

        // ===== Post-Phase: Verify =====
        auto t_verify = chrono::high_resolution_clock::now();
        double verify_cpu_time_ms = 0.0;
        bool valid = verifyColoring(graph, nodes, "Batch " + to_string(b), verify_cpu_time_ms);
        bm.Verify_CPU_post = chrono::duration<float, milli>(chrono::high_resolution_clock::now() - t_verify).count();
        bm.Verify_Util = (bm.Verify_CPU_post > 0) ?
            (float)(verify_cpu_time_ms / ((double)bm.Verify_CPU_post * n_threads)) * 100.0f : 0.0f;

        bm.time_total_batch = chrono::duration<float, milli>(chrono::high_resolution_clock::now() - t_batch_start).count();
        bm.iterations = itr;
        bm.chromatic_num = getChromaticNumber(nodes);

        chromatic_history.push_back(bm.chromatic_num);
        all_metrics.push_back(bm);

        // Live output
        cout << fixed << setprecision(3)
             << "Batch " << b << " (file: " << bm.filename << ")"
             << " | Chrom: " << bm.chromatic_num
             << " | Pre: " << bm.PrePhase_CPU << "ms"
             << " | P1: " << bm.ProcessCE_CPU_P1 << "ms"
             << " | P2: " << bm.CheckConflict_CPU_P2 << "ms"
             << " | P3: " << bm.UpdateNeighbors_CPU_P3 << "ms"
             << " | Verify: " << bm.Verify_CPU_post << "ms"
             << " | Iter: " << bm.iterations
             << " | Total: " << bm.time_total_batch << "ms"
             << (valid ? "" : " [INVALID]")
             << endl;
    }

    auto session_end = chrono::high_resolution_clock::now();
    float session_time_ms = chrono::duration<float, milli>(session_end - session_start).count();

    // ============ SUMMARY ============
    int cnt = (int)all_metrics.size();
    if (cnt == 0) {
        cout << "No batches processed." << endl;
        return 0;
    }

    float avg_pre = 0, avg_p1 = 0, avg_p2 = 0, avg_p3 = 0, avg_verify = 0, avg_total = 0;
    int tot_iters = 0;
    for (const auto& m : all_metrics) {
        avg_pre += m.PrePhase_CPU;
        avg_p1 += m.ProcessCE_CPU_P1;
        avg_p2 += m.CheckConflict_CPU_P2;
        avg_p3 += m.UpdateNeighbors_CPU_P3;
        avg_verify += m.Verify_CPU_post;
        avg_total += m.time_total_batch;
        tot_iters += m.iterations;
    }

    cout << endl;
    cout << "==================== SUMMARY ====================" << endl;
    cout << "Batches Processed:       " << cnt << endl;
    cout << "Selected Indices:        [";
    for (size_t i = 0; i < selected.size(); i++) {
        if (i > 0) cout << ", ";
        cout << selected[i];
    }
    cout << "]" << endl;
    cout << "Initial Chromatic Num:   " << chromatic_history.front() << endl;
    cout << "Final Chromatic Num:     " << chromatic_history.back() << endl;
    cout << "Avg Iterations/Batch:    " << fixed << setprecision(1) << (float)tot_iters / cnt << endl;
    cout << "Session Time:            " << fixed << setprecision(3) << session_time_ms << " ms" << endl;
    cout << "Avg Latency:             " << session_time_ms / cnt << " ms" << endl;
    cout << endl;
    cout << "--- Average Timings (ms) per Batch ---" << endl;
    cout << "PrePhase_CPU             " << avg_pre / cnt << endl;
    cout << "ProcessCE_CPU_P1         " << avg_p1 / cnt << endl;
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
                << "PrePhase_CPU,ProcessCE_CPU_P1,CheckConflict_CPU_P2,UpdateNeighbors_CPU_P3,"
                << "Verify_CPU_post,TotalBatch_ms,Iterations,ChromaticNum,"
                << "SessionTime_ms,AvgLatency_ms,"
                << "PrePhase_Util,P1_Util,P2_Util,P3_Util,Verify_Util" << endl;
        }

        // Extract dataset name and ratio from batch_folder path
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
                    if (pos != string::npos) {
                        dataset_name = bf.substr(pos + 1);
                    }
                }
            }
        }

        for (const auto& m : all_metrics) {
            csv << dataset_name << "," << batch_ratio << ","
                << m.id << "," << m.filename << ","
                << fixed << setprecision(3)
                << m.PrePhase_CPU << "," << m.ProcessCE_CPU_P1 << ","
                << m.CheckConflict_CPU_P2 << "," << m.UpdateNeighbors_CPU_P3 << ","
                << m.Verify_CPU_post << "," << m.time_total_batch << ","
                << m.iterations << "," << m.chromatic_num << ","
                << session_time_ms << "," << session_time_ms / cnt << ","
                << m.PrePhase_Util << "," << m.P1_Util << ","
                << m.P2_Util << "," << m.P3_Util << ","
                << m.Verify_Util << endl;
        }
        csv.close();
        cout << "CSV results appended to: " << csv_path << endl;
    }

    return 0;
}
