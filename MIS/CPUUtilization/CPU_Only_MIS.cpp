// CPU_Only_MIS.cpp — Dynamic MIS with Vertex Clustering (3-Phase Pipeline)
// Phase 1: BFS Neighborhood Computation
// Phase 2: Vertex Clustering
// Phase 3: MIS Computation (handleInsertion / handleDeletion)
//
// Compile: g++ -O2 -fopenmp -std=c++17 CPU_Only_MIS.cpp -o CPU_Only_MIS
// Run:     ./CPU_Only_MIS <MTX> <MIS_File> <BatchesFolder> <NumBatches> <Threads>

#include <omp.h>
#include <iostream>
#include <vector>
#include <queue>
#include <fstream>
#include <sstream>
#include <unordered_map>
#include <unordered_set>
#include <chrono>
#include <algorithm>
#include <numeric>
#include <random>
#include <iomanip>
#include <cmath>

using namespace std;

int n_threads = 32;

// ============================================================================
//                          DATA STRUCTURES
// ============================================================================

struct Node {
    int label;
    bool membership;
    Node(int l = 0, bool m = false) : label(l), membership(m) {}
};

class Graph {
public:
    int num_nodes;
    long long num_Edges = 0;
    vector<vector<int>> adj_list;

    Graph(int n = 0) : num_nodes(n), adj_list(n) {}

    void addEdge(int u, int v) {
        if (u >= 0 && u < num_nodes && v >= 0 && v < num_nodes) {
            adj_list[u].push_back(v);
            adj_list[v].push_back(u);
        }
    }

    void removeEdge(int u, int v) {
        if (u >= 0 && u < num_nodes && v >= 0 && v < num_nodes) {
            adj_list[u].erase(remove(adj_list[u].begin(), adj_list[u].end(), v), adj_list[u].end());
            adj_list[v].erase(remove(adj_list[v].begin(), adj_list[v].end(), u), adj_list[v].end());
        }
        num_Edges--;
    }

    void insertEdge(int u, int v) {
        addEdge(u, v);
        num_Edges++;
    }

    bool isAdjacent(int u, int v) const {
        if (u < 0 || u >= num_nodes) return false;
        for (int i = 0; i < (int)adj_list[u].size(); i++) {
            if (adj_list[u][i] == v) return true;
        }
        return false;
    }

    const vector<int>& getNeighbors(int node) const {
        static const vector<int> empty;
        if (node < 0 || node >= num_nodes) return empty;
        return adj_list[node];
    }
};

struct Edge {
    int source, destination;
    bool isInsertion;
    int clusterID;
    Edge(int s, int d, bool ins) : source(s), destination(d), isInsertion(ins), clusterID(-1) {}
};

class VertexCluster {
public:
    vector<int> vertices;
    vector<Edge> affected_edges;
    VertexCluster() {
        vertices.reserve(50);
        affected_edges.reserve(100);
    }
};

struct BatchMetrics {
    int id;
    string filename;
    float PrePhase_CPU;
    float BFS_CPU_P1;
    float Clustering_CPU_P2;
    float MISComp_CPU_P3;
    float Verify_CPU_post;
    float time_total_batch;
    int mis_cardinality;
    int num_clusters;
    float PrePhase_Util;
    float BFS_Util_P1;
    float Clust_Util_P2;
    float MIS_Util_P3;
    float Verify_Util;
};

// ============================================================================
//                          GRAPH I/O
// ============================================================================

Graph* read_Graph(const string& filename) {
    Graph* graph = new Graph();
    ifstream file(filename);
    if (!file.is_open()) {
        cerr << "Unable to open file: " << filename << endl;
        return graph;
    }

    string line;
    while (getline(file, line)) {
        if (line.empty() || line[0] == '%') continue;
        break; // dimension line
    }

    stringstream ss(line);
    long long nodes_ll, cols_ll, edgesCount_ll;
    ss >> nodes_ll >> cols_ll >> edgesCount_ll;
    int nodes = (int)nodes_ll;
    graph->num_nodes = nodes;
    graph->num_Edges = edgesCount_ll;
    graph->adj_list.resize(nodes);

    while (getline(file, line)) {
        if (line.empty()) continue;
        int u, v;
        stringstream ss2(line);
        ss2 >> u >> v;
        if (u >= 0 && u < nodes && v >= 0 && v < nodes) {
            graph->adj_list[u].push_back(v);
            graph->adj_list[v].push_back(u);
        }
    }
    return graph;
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
    iota(indices.begin(), indices.end(), 1); // 1..total
    mt19937 rng(seed);
    shuffle(indices.begin(), indices.end(), rng);
    indices.resize(min(select, total));
    sort(indices.begin(), indices.end());
    return indices;
}

// ============================================================================
//                          UTILITY FUNCTIONS
// ============================================================================

vector<int> extractVerticesFromEdges(const vector<Edge>& edges) {
    unordered_set<int> vertex_set;
    for (const Edge& edge : edges) {
        vertex_set.insert(edge.source);
        vertex_set.insert(edge.destination);
    }
    return vector<int>(vertex_set.begin(), vertex_set.end());
}

vector<Edge> getAffectedEdgesForVertexCluster(const vector<int>& cluster_vertices,
                                               const vector<Edge>& all_edges) {
    unordered_set<int> vertex_set(cluster_vertices.begin(), cluster_vertices.end());
    vector<Edge> affected_edges;
    for (const Edge& edge : all_edges) {
        if (vertex_set.count(edge.source) || vertex_set.count(edge.destination)) {
            affected_edges.push_back(edge);
        }
    }
    return affected_edges;
}

// ============================================================================
//               PHASE 1: BFS NEIGHBORHOOD COMPUTATION
// ============================================================================

vector<int> getVertexNeighborhood(int vertex, const Graph& graph, int hop_limit) {
    vector<int> neighborhood;
    neighborhood.reserve(20 * hop_limit);

    vector<bool> visited(graph.num_nodes, false);
    deque<pair<int, int>> q;

    q.push_back({vertex, 0});
    visited[vertex] = true;
    neighborhood.push_back(vertex);

    while (!q.empty()) {
        auto [current_vertex, hops] = q.front();
        q.pop_front();

        if (hops >= hop_limit) continue;

        const vector<int>& neighbors = graph.getNeighbors(current_vertex);
        for (int neighbor : neighbors) {
            if (!visited[neighbor]) {
                visited[neighbor] = true;
                neighborhood.push_back(neighbor);
                q.push_back({neighbor, hops + 1});
            }
        }
    }
    return neighborhood;
}

unordered_map<int, vector<int>> computeBFSNeighborhoods(
    const vector<Edge>& edges, const Graph& graph, int hop_limit,
    double& out_cpu_time_ms)
{
    vector<int> vertices = extractVerticesFromEdges(edges);
    unordered_map<int, vector<int>> neighborhoods;

    int nn_threads = ((int)vertices.size() / n_threads < 5) ? 1 : n_threads;
    double thread_cpu = 0.0;

    #pragma omp parallel for num_threads(nn_threads) schedule(dynamic) reduction(+:thread_cpu)
    for (size_t i = 0; i < vertices.size(); ++i) {
        double t0 = omp_get_wtime();
        int vertex = vertices[i];
        vector<int> nbhood = getVertexNeighborhood(vertex, graph, hop_limit);
        #pragma omp critical
        {
            neighborhoods[vertex] = move(nbhood);
        }
        thread_cpu += (omp_get_wtime() - t0) * 1000.0;
    }
    out_cpu_time_ms = thread_cpu;
    return neighborhoods;
}

// ============================================================================
//               PHASE 2: VERTEX CLUSTERING
// ============================================================================

vector<VertexCluster> clusterVertices(
    const vector<Edge>& edges, const Graph& graph,
    const unordered_map<int, vector<int>>& neighborhoods)
{
    vector<int> vertices = extractVerticesFromEdges(edges);

    vector<VertexCluster> clusters;
    vector<int> nodeToCluster(graph.num_nodes, -1);
    int nextClusterId = 0;

    for (int vertex : vertices) {
        if (nodeToCluster[vertex] != -1) continue;

        auto it = neighborhoods.find(vertex);
        if (it == neighborhoods.end()) continue;
        const vector<int>& neighborhood = it->second;

        // Find overlapping clusters
        unordered_set<int> overlapping_clusters;
        for (int neighbor : neighborhood) {
            if (neighbor < graph.num_nodes && nodeToCluster[neighbor] != -1) {
                overlapping_clusters.insert(nodeToCluster[neighbor]);
            }
        }

        int finalClusterId;

        if (overlapping_clusters.empty()) {
            finalClusterId = nextClusterId++;
            clusters.resize(nextClusterId);
        } else if (overlapping_clusters.size() == 1) {
            finalClusterId = *overlapping_clusters.begin();
        } else {
            // Find largest cluster
            finalClusterId = *overlapping_clusters.begin();
            size_t maxSize = clusters[finalClusterId].vertices.size();
            for (int clusterId : overlapping_clusters) {
                if (clusterId < (int)clusters.size() && clusters[clusterId].vertices.size() > maxSize) {
                    maxSize = clusters[clusterId].vertices.size();
                    finalClusterId = clusterId;
                }
            }

            // Merge all other clusters into target
            for (int clusterId : overlapping_clusters) {
                if (clusterId != finalClusterId && clusterId < (int)clusters.size()) {
                    clusters[finalClusterId].vertices.insert(
                        clusters[finalClusterId].vertices.end(),
                        make_move_iterator(clusters[clusterId].vertices.begin()),
                        make_move_iterator(clusters[clusterId].vertices.end())
                    );
                    for (int v : clusters[clusterId].vertices) {
                        if (v < (int)nodeToCluster.size()) {
                            nodeToCluster[v] = finalClusterId;
                        }
                    }
                    clusters[clusterId] = VertexCluster();
                }
            }
        }

        clusters[finalClusterId].vertices.push_back(vertex);
        for (int neighbor : neighborhood) {
            if (neighbor < (int)nodeToCluster.size()) {
                nodeToCluster[neighbor] = finalClusterId;
            }
        }
    }

    // Assign affected edges to each cluster
    for (auto& cluster : clusters) {
        if (!cluster.vertices.empty()) {
            cluster.affected_edges = getAffectedEdgesForVertexCluster(cluster.vertices, edges);
        }
    }

    // Remove empty clusters
    clusters.erase(
        remove_if(clusters.begin(), clusters.end(),
                  [](const VertexCluster& c) { return c.vertices.empty(); }),
        clusters.end());

    return clusters;
}

// ============================================================================
//               PHASE 3: MIS COMPUTATION (Lemma-based)
// ============================================================================

void handleInsertion(const Edge& edge, Graph& graph, vector<Node>& nodes) {
    int u = edge.source;
    int v = edge.destination;

    bool u_in_mis = nodes[u].membership;
    bool v_in_mis = nodes[v].membership;

    if (u_in_mis && v_in_mis) {
        // Lemma 1: Both in MIS, remove smaller node
        v = min(u, v);
        nodes[v].membership = false;

        for (int neighbor : graph.getNeighbors(v)) {
            if (!nodes[neighbor].membership) {
                bool can_add = true;
                for (int nbr_of_nbr : graph.getNeighbors(neighbor)) {
                    if (nodes[nbr_of_nbr].membership) {
                        can_add = false;
                        break;
                    }
                }
                if (can_add) {
                    nodes[neighbor].membership = true;
                }
            }
        }
    }
    // Lemma 2 & 3: At least one or neither in MIS — do nothing
}

void handleDeletion(const Edge& edge, Graph& graph, vector<Node>& nodes) {
    int u = edge.source;
    int v = edge.destination;

    bool u_in_mis = nodes[u].membership;
    bool v_in_mis = nodes[v].membership;

    if (u_in_mis || v_in_mis) {
        // Lemma 4: One in MIS, try to add the other
        if (u_in_mis && !v_in_mis) {
            bool can_add = true;
            for (int neighbor : graph.getNeighbors(v)) {
                if (nodes[neighbor].membership && neighbor != u) {
                    can_add = false;
                    break;
                }
            }
            if (can_add) nodes[v].membership = true;
        } else if (!u_in_mis && v_in_mis) {
            bool can_add = true;
            for (int neighbor : graph.getNeighbors(u)) {
                if (nodes[neighbor].membership && neighbor != v) {
                    can_add = false;
                    break;
                }
            }
            if (can_add) nodes[u].membership = true;
        }
    }
    // Lemma 5 & 6: Neither in MIS / both maintained — do nothing
}

// ============================================================================
//               POST-PHASE: MIS VERIFICATION
// ============================================================================

bool verifyMIS(const Graph& graph, const vector<Node>& nodes, const string& label,
               double& out_cpu_time_ms) {
    long long violations_independence = 0;
    long long violations_maximality = 0;
    double thread_cpu = 0.0;

    #pragma omp parallel for num_threads(n_threads) reduction(+:violations_independence, violations_maximality, thread_cpu)
    for (int u = 0; u < graph.num_nodes; u++) {
        double t0 = omp_get_wtime();
        if (nodes[u].membership) {
            for (int v : graph.adj_list[u]) {
                if (u < v && nodes[v].membership) {
                    violations_independence++;
                }
            }
        } else {
            if (!graph.adj_list[u].empty()) {
                bool has_mis_neighbor = false;
                for (int v : graph.adj_list[u]) {
                    if (nodes[v].membership) {
                        has_mis_neighbor = true;
                        break;
                    }
                }
                if (!has_mis_neighbor) {
                    violations_maximality++;
                }
            }
        }
        thread_cpu += (omp_get_wtime() - t0) * 1000.0;
    }

    out_cpu_time_ms = thread_cpu;

    if (violations_independence > 0 || violations_maximality > 0) {
        cout << "[FAILED] " << label << ": "
             << violations_independence << " independence, "
             << violations_maximality << " maximality violations" << endl;
        return false;
    }
    return true;
}

int countMIS(const vector<Node>& nodes) {
    int count = 0;
    for (const auto& n : nodes) {
        if (n.membership) count++;
    }
    return count;
}

// ============================================================================
//                              MAIN
// ============================================================================

int main(int argc, char* argv[]) {
    if (argc < 6) {
        cerr << "Usage: " << argv[0] << " <MTX> <MIS_File> <BatchesFolder> <NumBatches> <Threads>" << endl;
        return 1;
    }

    string mtx_path = argv[1];
    string mis_path = argv[2];
    string batch_folder = argv[3];
    int num_batches = stoi(argv[4]);
    n_threads = stoi(argv[5]);

    int hop_limit = 2;

    cout << "==================== CPU_Only_MIS ====================" << endl;
    cout << "MTX:     " << mtx_path << endl;
    cout << "MIS:     " << mis_path << endl;
    cout << "Batches: " << batch_folder << endl;
    cout << "NumBatch:" << num_batches << endl;
    cout << "Threads: " << n_threads << endl;
    cout << "======================================================" << endl;

    // ---- Load Graph ----
    Graph graph = *read_Graph(mtx_path);
    int num_nodes = graph.num_nodes;
    cout << "Nodes: " << num_nodes << ", Edges: " << (long long)graph.num_Edges << endl;

    // ---- Initialize Nodes ----
    vector<Node> nodes(num_nodes);
    for (int i = 0; i < num_nodes; ++i)
        nodes[i] = Node(i, false);

    // ---- Load Initial MIS ----
    ifstream mis_file(mis_path);
    int initial_card = 0;
    if (mis_file.is_open()) {
        string line;
        while (getline(mis_file, line)) {
            if (line.empty()) continue;
            int v = stoi(line);
            if (v + 1 < num_nodes) {
                nodes[v + 1].membership = true;
                initial_card++;
            }
        }
        mis_file.close();
    } else {
        cerr << "ERROR: Cannot open MIS file: " << mis_path << endl;
        return 1;
    }
    cout << "Initial MIS Cardinality: " << initial_card << endl;

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

    auto session_start = chrono::high_resolution_clock::now();

    for (int b = 0; b < (int)selected.size(); b++) {
        int batch_idx = selected[b];
        string batch_path = batch_folder + "/" + to_string(batch_idx) + ".mtx";

        auto t_batch_start = chrono::high_resolution_clock::now();
        BatchMetrics bm;
        bm.id = b;
        bm.filename = to_string(batch_idx) + ".mtx";

        // ============ PRE-PHASE: Read + Sort + Topology Update ============
        auto t_pre = chrono::high_resolution_clock::now();

        // Read batch file
        vector<Edge> batch;
        ifstream bf(batch_path);
        if (!bf.is_open()) {
            cout << "WARNING: " << batch_path << " not found, skipping" << endl;
            continue;
        }
        string line;
        while (getline(bf, line)) {
            if (line.empty()) continue;
            stringstream ss(line);
            int src, dest;
            if (ss >> src >> dest) {
                bool isIns = !graph.isAdjacent(src, dest);
                batch.emplace_back(src, dest, isIns);
            }
        }
        bf.close();

        // Sort: deletions first, then insertions
        sort(batch.begin(), batch.end(), [](const Edge& a, const Edge& b) {
            return a.isInsertion < b.isInsertion;
        });

        bm.PrePhase_CPU = chrono::duration<float, milli>(
            chrono::high_resolution_clock::now() - t_pre).count();
        bm.PrePhase_Util = (1.0f / n_threads) * 100.0f; // sequential

        // ============ PHASE 1: BFS Neighborhood Computation ============
        // BFS runs on ORIGINAL graph (before topology update)
        auto t_p1 = chrono::high_resolution_clock::now();
        double bfs_cpu_time_ms = 0.0;
        auto neighborhoods = computeBFSNeighborhoods(batch, graph, hop_limit, bfs_cpu_time_ms);
        bm.BFS_CPU_P1 = chrono::duration<float, milli>(
            chrono::high_resolution_clock::now() - t_p1).count();
        bm.BFS_Util_P1 = (bm.BFS_CPU_P1 > 0) ?
            (float)(bfs_cpu_time_ms / ((double)bm.BFS_CPU_P1 * n_threads)) * 100.0f : 0.0f;

        // ============ PHASE 2: Vertex Clustering ============
        auto t_p2 = chrono::high_resolution_clock::now();
        vector<VertexCluster> clusters = clusterVertices(batch, graph, neighborhoods);
        bm.Clustering_CPU_P2 = chrono::duration<float, milli>(
            chrono::high_resolution_clock::now() - t_p2).count();
        bm.num_clusters = (int)clusters.size();
        bm.Clust_Util_P2 = (1.0f / n_threads) * 100.0f; // sequential

        // ============ PHASE 3: MIS Computation + Topology Update ============
        auto t_p3 = chrono::high_resolution_clock::now();
        double p3_cpu_time_ms = 0.0;

        int block = (int)ceil((float)clusters.size() / n_threads);

        #pragma omp parallel num_threads(n_threads) reduction(+:p3_cpu_time_ms)
        {
            double t0 = omp_get_wtime();
            int tid = omp_get_thread_num();
            int start = tid * block;
            int end = min((tid + 1) * block, (int)clusters.size());

            for (int i = start; i < end; ++i) {
                VertexCluster& cluster = clusters[i];
                for (size_t j = 0; j < cluster.affected_edges.size(); ++j) {
                    Edge& edge = cluster.affected_edges[j];
                    #pragma omp critical
                    {
                        if (edge.isInsertion) {
                            handleInsertion(edge, graph, nodes);
                            graph.insertEdge(edge.source, edge.destination);
                        } else {
                            handleDeletion(edge, graph, nodes);
                            graph.removeEdge(edge.source, edge.destination);
                        }
                    }
                }
            }
            p3_cpu_time_ms += (omp_get_wtime() - t0) * 1000.0;
        }

        bm.MISComp_CPU_P3 = chrono::duration<float, milli>(
            chrono::high_resolution_clock::now() - t_p3).count();
        bm.MIS_Util_P3 = (bm.MISComp_CPU_P3 > 0) ?
            (float)(p3_cpu_time_ms / ((double)bm.MISComp_CPU_P3 * n_threads)) * 100.0f : 0.0f;

        // ============ POST-PHASE: Verification ============
        auto t_post = chrono::high_resolution_clock::now();
        double verify_cpu_time_ms = 0.0;
        bool valid = verifyMIS(graph, nodes, "Batch " + to_string(b), verify_cpu_time_ms);
        bm.Verify_CPU_post = chrono::duration<float, milli>(
            chrono::high_resolution_clock::now() - t_post).count();
        bm.Verify_Util = (bm.Verify_CPU_post > 0) ?
            (float)(verify_cpu_time_ms / ((double)bm.Verify_CPU_post * n_threads)) * 100.0f : 0.0f;

        bm.mis_cardinality = countMIS(nodes);
        bm.time_total_batch = chrono::duration<float, milli>(
            chrono::high_resolution_clock::now() - t_batch_start).count();

        all_metrics.push_back(bm);

        // Live output
        cout << "Batch " << b << " (file: " << bm.filename << ")"
             << " | Card: " << bm.mis_cardinality
             << " | Clusters: " << bm.num_clusters
             << " | Pre: " << fixed << setprecision(3) << bm.PrePhase_CPU << "ms"
             << " | BFS: " << bm.BFS_CPU_P1 << "ms"
             << " | Clust: " << bm.Clustering_CPU_P2 << "ms"
             << " | MIS: " << bm.MISComp_CPU_P3 << "ms"
             << " | Verify: " << bm.Verify_CPU_post << "ms"
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

    float avg_pre = 0, avg_bfs = 0, avg_clust = 0, avg_mis = 0, avg_verify = 0, avg_total = 0;
    for (const auto& m : all_metrics) {
        avg_pre += m.PrePhase_CPU;
        avg_bfs += m.BFS_CPU_P1;
        avg_clust += m.Clustering_CPU_P2;
        avg_mis += m.MISComp_CPU_P3;
        avg_verify += m.Verify_CPU_post;
        avg_total += m.time_total_batch;
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
    cout << "Initial MIS Cardinality: " << initial_card << endl;
    cout << "Final MIS Cardinality:   " << all_metrics.back().mis_cardinality << endl;
    cout << "Session Time:            " << fixed << setprecision(3) << session_time_ms << " ms" << endl;
    cout << "Avg Latency:             " << session_time_ms / cnt << " ms" << endl;
    cout << endl;
    cout << "--- Average Timings (ms) per Batch ---" << endl;
    cout << "PrePhase_CPU             " << avg_pre / cnt << endl;
    cout << "BFS_CPU_P1               " << avg_bfs / cnt << endl;
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
        // Write header if file is new
        if (!csv_exists) {
            csv << "Dataset,BatchRatio,BatchID,BatchFile,"
                << "PrePhase_CPU,BFS_CPU_P1,Clustering_CPU_P2,MISComp_CPU_P3,"
                << "Verify_CPU_post,TotalBatch_ms,MIS_Cardinality,NumClusters,"
                << "SessionTime_ms,AvgLatency_ms,"
                << "PrePhase_Util,BFS_Util_P1,Clust_Util_P2,MIS_Util_P3,Verify_Util" << endl;
        }

        // Extract dataset name and ratio from paths
        // batch_folder looks like: .../preprocessed/dataset/Batches/ratio
        string dataset_name = "unknown", batch_ratio = "unknown";
        {
            // Parse batch_folder to extract dataset and ratio
            string bf = batch_folder;
            // Remove trailing slash if present
            if (!bf.empty() && bf.back() == '/') bf.pop_back();
            // ratio is last component
            size_t pos = bf.rfind('/');
            if (pos != string::npos) {
                batch_ratio = bf.substr(pos + 1);
                bf = bf.substr(0, pos);
                // skip "Batches"
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
                << m.PrePhase_CPU << "," << m.BFS_CPU_P1 << ","
                << m.Clustering_CPU_P2 << "," << m.MISComp_CPU_P3 << ","
                << m.Verify_CPU_post << "," << m.time_total_batch << ","
                << m.mis_cardinality << "," << m.num_clusters << ","
                << session_time_ms << "," << session_time_ms / cnt << ","
                << m.PrePhase_Util << "," << m.BFS_Util_P1 << ","
                << m.Clust_Util_P2 << "," << m.MIS_Util_P3 << ","
                << m.Verify_Util << endl;
        }
        csv.close();
        cout << "CSV results appended to: " << csv_path << endl;
    }

    return 0;
}
