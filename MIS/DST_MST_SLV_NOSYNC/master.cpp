/*
 * master.cpp - Master node for DST_MST_SLV_NOSYNC MIS Pipeline
 *
 * NO-SYNC variant: slaves do local MIS updates, master applies deltas
 * directly without boundary conflict resolution or ghost refreshes.
 *
 * Usage: ./master <graph.mtx> <mis_file> <batch_dir> <num_batches> <num_threads> <K> slave1:port slave2:port
 */

#include <iostream>
#include <vector>
#include <algorithm>
#include <chrono>
#include <iomanip>
#include <cstring>
#include <unordered_set>
#include <fstream>
#include <sstream>
#include <cmath>
#include <thread>
#include <numeric>
#include <random>
#include <unistd.h>

#include "protocol.h"
#include "graph_utils.h"
#include "partitioner.h"

using namespace std;

/* ---- Batch file reading ---- */

static int countBatchFiles(const string& folder) {
    int count = 0;
    for (int i = 1; i <= 1000; i++) {
        string path = folder + "/" + to_string(i) + ".mtx";
        ifstream f(path);
        if (f.good()) count++;
        else if (i > count + 5) break;
    }
    return count;
}

static vector<int> selectBatches(int total, int select, unsigned seed = 42) {
    vector<int> indices(total);
    iota(indices.begin(), indices.end(), 1);
    mt19937 rng(seed);
    shuffle(indices.begin(), indices.end(), rng);
    indices.resize(min(select, total));
    sort(indices.begin(), indices.end());
    return indices;
}

static vector<RawEdge> readBatchFile(const string& path) {
    vector<RawEdge> edges;
    ifstream f(path);
    if (!f.is_open()) return edges;
    string line;
    while (getline(f, line)) {
        if (line.empty()) continue;
        istringstream ss(line);
        uint32_t u, v;
        if (ss >> u >> v) edges.push_back({u, v});
    }
    return edges;
}

/* ===========================================================================
 * MAIN
 * =========================================================================== */

int main(int argc, char *argv[]) {
    if (argc < 8) {
        cerr << "Usage: " << argv[0] << " <graph.mtx> <mis_file> <batch_dir> "
             << "<num_batches> <num_threads> <K> slave1:port [slave2:port ...]" << endl;
        return 1;
    }

    string input_file = argv[1];
    string mis_file = argv[2];
    string batch_dir = argv[3];
    int num_batches = stoi(argv[4]);
    int num_threads = stoi(argv[5]);
    int K = stoi(argv[6]);

    vector<string> slave_addrs;
    for (int i = 7; i < argc && (int)slave_addrs.size() < K; i++)
        slave_addrs.push_back(argv[i]);

    if ((int)slave_addrs.size() < K) {
        cerr << "ERROR: Need " << K << " slave addresses" << endl;
        return 1;
    }

    string dataset_name = extract_dataset_name(input_file);

    cout << "================================================================================" << endl;
    cout << " DST_MST_SLV_NOSYNC MIS Pipeline - MASTER (NO sync, NO ghost refresh)" << endl;
    cout << "================================================================================" << endl;
    cout << "Dataset:       " << dataset_name << endl;
    cout << "MIS file:      " << mis_file << endl;
    cout << "Batch dir:     " << batch_dir << endl;
    cout << "Num batches:   " << num_batches << endl;
    cout << "Partitions:    " << K << endl;
    cout << "Threads:       " << num_threads << endl;
    cout << "DPU offload:   BFS + Clustering (on ARM Cortex-A72)" << endl;
    cout << "Sync mode:     NONE (no conflict resolution, no ghost refresh)" << endl;
    cout << "================================================================================" << endl << endl;

    auto t_total_start = chrono::high_resolution_clock::now();

    /* ================================================================
     * PHASE 1: PREPROCESSING
     * ================================================================ */
    cout << "=== Phase 1: Preprocessing ===" << endl;
    auto t0 = chrono::high_resolution_clock::now();

    EdgeListData raw = read_raw_edges(input_file);
    uint32_t num_nodes = raw.num_nodes;

    add_backedges(raw.edges);

    vector<vector<uint32_t>> adj;
    build_sorted_adj(num_nodes, raw.edges, adj);
    raw.edges.clear();

    /* Load MIS from file */
    vector<uint8_t> membership(num_nodes, 0);
    {
        ifstream mf(mis_file);
        if (!mf.is_open()) {
            cerr << "ERROR: Cannot open MIS file: " << mis_file << endl;
            return 1;
        }
        string line;
        uint32_t idx = 0;
        while (getline(mf, line)) {
            if (line.empty()) continue;
            if (idx < num_nodes) membership[idx++] = (uint8_t)stoi(line);
        }
        cout << "[Master] Loaded MIS for " << idx << " vertices" << endl;
    }

    int initial_card = 0;
    for (uint32_t i = 0; i < num_nodes; i++) if (membership[i]) initial_card++;
    cout << "[Master] Initial MIS cardinality: " << initial_card << endl;

    auto t1 = chrono::high_resolution_clock::now();
    double preprocess_ms = chrono::duration_cast<chrono::microseconds>(t1 - t0).count() / 1000.0;
    cout << "[Master] Preprocessing: " << fixed << setprecision(1) << preprocess_ms << " ms" << endl;

    /* ================================================================
     * PHASE 2: PARTITIONING
     * ================================================================ */
    cout << "\n=== Phase 2: METIS Partitioning ===" << endl;
    auto t_part0 = chrono::high_resolution_clock::now();

    PartitionResult part_result = partition_graph(num_nodes, adj, K);

    vector<SubGraph> subgraphs(K);
    for (int p = 0; p < K; p++)
        subgraphs[p] = extract_subgraph(num_nodes, adj, membership, part_result.part, p);

    auto t_part1 = chrono::high_resolution_clock::now();
    double partition_ms = chrono::duration_cast<chrono::microseconds>(t_part1 - t_part0).count() / 1000.0;
    cout << "[Master] Partitioning: " << fixed << setprecision(1) << partition_ms << " ms" << endl;

    /* ================================================================
     * PHASE 3: CONNECT TO SLAVES + DISTRIBUTE
     * ================================================================ */
    cout << "\n=== Phase 3: Connecting to Slaves ===" << endl;
    auto t_dist0 = chrono::high_resolution_clock::now();

    vector<int> slave_socks(K);
    for (int p = 0; p < K; p++) {
        auto [host, port] = parse_host_port(slave_addrs[p]);
        cout << "[Master] Connecting to slave " << p << " at " << host << ":" << port << "..." << endl;

        int retries = 30;
        while (retries > 0) {
            try {
                slave_socks[p] = tcp_connect(host, port);
                break;
            } catch (exception &e) {
                retries--;
                if (retries == 0) {
                    cerr << "[Master] ERROR: Cannot connect to slave " << p << endl;
                    return 1;
                }
                usleep(1000000);
            }
        }
        cout << "[Master] Connected to slave " << p << endl;

        send_partition(slave_socks[p], subgraphs[p], p, K, num_nodes);
        cout << "[Master] Sent partition " << p
             << " (nv=" << subgraphs[p].local_nv
             << " ne=" << subgraphs[p].local_ne << ")" << endl;
    }

    auto t_dist1 = chrono::high_resolution_clock::now();
    double dist_ms = chrono::duration_cast<chrono::microseconds>(t_dist1 - t_dist0).count() / 1000.0;

    /* Receive slave setup timings (5 doubles) */
    vector<double> slave_recv_part(K), slave_ghost_init(K), slave_host_graph(K);
    vector<double> slave_connect_dpu(K), slave_send_graph(K);
    for (int p = 0; p < K; p++) {
        slave_recv_part[p] = recv_double(slave_socks[p]);
        slave_ghost_init[p] = recv_double(slave_socks[p]);
        slave_host_graph[p] = recv_double(slave_socks[p]);
        slave_connect_dpu[p] = recv_double(slave_socks[p]);
        slave_send_graph[p] = recv_double(slave_socks[p]);
    }

    /* ================================================================
     * PHASE 4: BATCH LOOP (NO SYNC)
     * ================================================================ */
    cout << "\n=== Phase 4: Batch Processing (NO SYNC) ===" << endl;

    int total_batch_files = countBatchFiles(batch_dir);
    vector<int> selected = selectBatches(total_batch_files, num_batches);
    cout << "[Master] Selected " << selected.size() << " batches" << endl;

    double total_wall_ms = 0;
    int batches_completed = 0;
    int total_deltas_applied = 0;

    /* Priming batch per-partition */
    vector<double> prime_send_v(K, 0), prime_dpu_bfs_v(K, 0), prime_dpu_clust_v(K, 0);
    vector<double> prime_recv_v(K, 0), prime_wall_v(K, 0);

    /* Steady-state per-partition accumulators */
    vector<double> ss_send_v(K, 0), ss_dpu_bfs_v(K, 0), ss_dpu_clust_v(K, 0);
    vector<double> ss_recv_v(K, 0), ss_mis_v(K, 0), ss_wall_v(K, 0);
    int ss_count = 0;

    for (int round = 0; round < (int)selected.size(); round++) {
        int batch_idx = selected[round];
        string batch_path = batch_dir + "/" + to_string(batch_idx) + ".mtx";

        auto t_round0 = chrono::high_resolution_clock::now();

        vector<RawEdge> all_edges = readBatchFile(batch_path);
        if (all_edges.empty()) continue;

        /* Partition edges */
        vector<vector<RawEdge>> batch_per_partition(K);
        for (auto &e : all_edges) {
            if (e.src >= num_nodes || e.dst >= num_nodes) continue;
            int p_src = part_result.part[e.src];
            int p_dst = part_result.part[e.dst];
            batch_per_partition[p_src].push_back(e);
            if (p_dst != p_src)
                batch_per_partition[p_dst].push_back(e);
        }

        /* Send batch edges to each slave */
        for (int p = 0; p < K; p++) {
            uint32_t ne = (uint32_t)batch_per_partition[p].size();
            send_u32(slave_socks[p], ne);
            if (ne > 0)
                send_all(slave_socks[p], batch_per_partition[p].data(), ne * sizeof(RawEdge));
        }

        /* Update master's adj with batch edges */
        for (auto &e : all_edges) {
            if (e.src < num_nodes && e.dst < num_nodes) {
                bool is_adj = binary_search(adj[e.src].begin(), adj[e.src].end(), e.dst);
                if (!is_adj) {
                    auto it = lower_bound(adj[e.src].begin(), adj[e.src].end(), e.dst);
                    adj[e.src].insert(it, e.dst);
                    it = lower_bound(adj[e.dst].begin(), adj[e.dst].end(), e.src);
                    adj[e.dst].insert(it, e.src);
                } else {
                    auto it = lower_bound(adj[e.src].begin(), adj[e.src].end(), e.dst);
                    if (it != adj[e.src].end() && *it == e.dst) adj[e.src].erase(it);
                    it = lower_bound(adj[e.dst].begin(), adj[e.dst].end(), e.src);
                    if (it != adj[e.dst].end() && *it == e.src) adj[e.dst].erase(it);
                }
            }
        }

        /* Receive boundary deltas + timing from all slaves (6 doubles each) */
        vector<double> slave_wall(K), slave_dpu_bfs(K), slave_dpu_clust(K);
        vector<double> slave_mis(K), slave_send(K), slave_recv(K);
        int round_deltas = 0;

        for (int p = 0; p < K; p++) {
            uint32_t num_deltas = recv_u32(slave_socks[p]);
            if (num_deltas > 0) {
                vector<BoundaryDelta> deltas(num_deltas);
                recv_all(slave_socks[p], deltas.data(), num_deltas * sizeof(BoundaryDelta));
                /* Apply directly — NO conflict resolution */
                for (auto &d : deltas)
                    if (d.global_node_id < num_nodes)
                        membership[d.global_node_id] = d.new_membership;
                round_deltas += num_deltas;
            }
            slave_wall[p] = recv_double(slave_socks[p]);
            slave_dpu_bfs[p] = recv_double(slave_socks[p]);
            slave_dpu_clust[p] = recv_double(slave_socks[p]);
            slave_mis[p] = recv_double(slave_socks[p]);
            slave_send[p] = recv_double(slave_socks[p]);
            slave_recv[p] = recv_double(slave_socks[p]);
        }

        total_deltas_applied += round_deltas;

        /* NO conflict resolution */
        /* NO corrections sent to slaves */
        /* NO ghost refresh */

        /* Accumulate priming vs steady-state */
        if (round == 0) {
            for (int p = 0; p < K; p++) {
                prime_send_v[p] = slave_send[p];
                prime_dpu_bfs_v[p] = slave_dpu_bfs[p];
                prime_dpu_clust_v[p] = slave_dpu_clust[p];
                prime_recv_v[p] = slave_recv[p];
                prime_wall_v[p] = slave_wall[p];
            }
        } else {
            for (int p = 0; p < K; p++) {
                ss_send_v[p] += slave_send[p];
                ss_dpu_bfs_v[p] += slave_dpu_bfs[p];
                ss_dpu_clust_v[p] += slave_dpu_clust[p];
                ss_recv_v[p] += slave_recv[p];
                ss_mis_v[p] += slave_mis[p];
                ss_wall_v[p] += slave_wall[p];
            }
            ss_count++;
        }

        auto t_round1 = chrono::high_resolution_clock::now();
        double round_ms = chrono::duration_cast<chrono::microseconds>(t_round1 - t_round0).count() / 1000.0;
        total_wall_ms += round_ms;
        batches_completed++;

        cout << "[Batch " << (round+1) << "/" << selected.size() << "] wall=" << fixed << setprecision(3) << round_ms << "ms";
        for (int p = 0; p < K; p++)
            cout << " P" << p << "={bfs=" << slave_dpu_bfs[p]
                 << "ms,clust=" << slave_dpu_clust[p]
                 << "ms,mis=" << slave_mis[p] << "ms}";
        cout << " deltas=" << round_deltas << endl;
    }

    /* ================================================================
     * PHASE 5: SHUTDOWN
     * ================================================================ */
    cout << "\n=== Phase 5: Shutdown ===" << endl;

    for (int p = 0; p < K; p++)
        send_u32(slave_socks[p], SHUTDOWN_SENTINEL);

    for (int p = 0; p < K; p++) {
        auto final_mem = recv_vec<DeltaEntry>(slave_socks[p]);
        for (auto &d : final_mem)
            if (d.node_id < num_nodes)
                membership[d.node_id] = (uint8_t)d.new_val;
        close(slave_socks[p]);
    }

    /* NO final MIS repair — report as-is */
    int final_card = 0;
    for (uint32_t i = 0; i < num_nodes; i++) if (membership[i]) final_card++;
    bool mis_valid = verify_mis(adj, membership, num_nodes);

    auto t_total_end = chrono::high_resolution_clock::now();
    double total_ms = chrono::duration_cast<chrono::microseconds>(t_total_end - t_total_start).count() / 1000.0;

    cout << endl;
    cout << "================================================================================" << endl;
    cout << "             DST_MST_SLV_NOSYNC MIS PROFILING RESULTS" << endl;
    cout << "================================================================================" << endl;
    cout << "Initial MIS cardinality: " << initial_card << endl;
    cout << "Final MIS cardinality:   " << final_card << endl;
    cout << "MIS valid:               " << (mis_valid ? "YES" : "NO") << endl;
    cout << "Total deltas applied:    " << total_deltas_applied << endl;
    cout << "Total time:              " << fixed << setprecision(3) << total_ms << " ms" << endl;
    if (batches_completed > 0)
        cout << "Avg wall/batch:          " << total_wall_ms / batches_completed << " ms" << endl;
    cout << "================================================================================" << endl;

    /* Write CSV */
    string csv_path = "results.csv";
    bool csv_exists = ifstream(csv_path).good();
    ofstream csv(csv_path, ios::app);
    if (csv.is_open()) {
        if (!csv_exists) {
            csv << "Dataset,BatchRatio,InitCard,FinalCard,MISValid,"
                << "Preprocess_ms,Partition_ms,Distribution_ms,"
                << "AvgDpuBfs_P0,AvgDpuClust_P0,AvgMIS_P0,AvgWall_P0,"
                << "AvgDpuBfs_P1,AvgDpuClust_P1,AvgMIS_P1,AvgWall_P1,"
                << "TotalBatch_ms,TotalTime_ms,TotalDeltasApplied" << endl;
        }

        string batch_ratio = "unknown";
        {
            string bd = batch_dir;
            if (!bd.empty() && bd.back() == '/') bd.pop_back();
            size_t pos = bd.rfind('/');
            if (pos != string::npos) batch_ratio = bd.substr(pos + 1);
        }

        csv << dataset_name << "," << batch_ratio << ","
            << initial_card << "," << final_card << "," << (mis_valid ? 1 : 0) << ","
            << fixed << setprecision(3)
            << preprocess_ms << "," << partition_ms << "," << dist_ms << ",";

        for (int p = 0; p < min(K, 2); p++) {
            if (ss_count > 0) {
                csv << ss_dpu_bfs_v[p]/ss_count << ","
                    << ss_dpu_clust_v[p]/ss_count << ","
                    << ss_mis_v[p]/ss_count << ","
                    << ss_wall_v[p]/ss_count << ",";
            } else {
                csv << "0,0,0,0,";
            }
        }

        csv << total_wall_ms << "," << total_ms << "," << total_deltas_applied << endl;
    }

    return 0;
}
