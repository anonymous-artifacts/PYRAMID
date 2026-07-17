/*
 * master.cpp - Master node for DST_MST_SLV Graph Coloring Pipeline
 *
 * Runs on login node. Connects to slave processes on cn04/cn05 via TCP.
 * Slaves offload ProcessCE (P1) + CheckConflict (P2) to DPU,
 * do UpdateNeighbors (P3) on CPU.
 * Uses preprocessed batch files and coloring file.
 *
 * Usage: ./master <graph.mtx> <colors_file> <batch_dir> <num_batches> <num_threads> <K> slave1:port slave2:port
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

#define MAX_COLORS 1024

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

/* ---- Smallest available color ---- */

static int32_t smallest_available_color(uint32_t u, const vector<vector<uint32_t>> &adj,
                                         const vector<int32_t> &colors, uint32_t nv) {
    uint8_t used[MAX_COLORS];
    memset(used, 0, MAX_COLORS);
    for (uint32_t nb : adj[u]) {
        if (nb < nv && colors[nb] >= 0 && colors[nb] < MAX_COLORS)
            used[colors[nb]] = 1;
    }
    for (int32_t c = 0; c < MAX_COLORS; c++)
        if (!used[c]) return c;
    return MAX_COLORS;
}

/* ---- Color conflict resolution at boundaries ---- */

struct ColorResolutionResult {
    vector<DeltaEntry> corrections;
    int iterations;
};

static ColorResolutionResult resolve_boundary_color_conflicts(
        const vector<vector<DeltaEntry>> &all_deltas,
        vector<int32_t> &master_colors,
        const vector<vector<uint32_t>> &adj,
        uint32_t nv) {

    ColorResolutionResult result;
    result.iterations = 0;

    vector<int32_t> working(master_colors);
    for (auto &partition_deltas : all_deltas)
        for (auto &d : partition_deltas)
            if (d.node_id < nv)
                working[d.node_id] = (int32_t)d.new_val;

    const int MAX_ITERATIONS = 50;

    while (result.iterations < MAX_ITERATIONS) {
        result.iterations++;
        bool changed = false;

        for (uint32_t u = 0; u < nv; u++) {
            if (working[u] < 0) continue;
            for (uint32_t v : adj[u]) {
                if (v <= u || v >= nv) continue;
                if (working[u] == working[v]) {
                    uint32_t y = v;
                    working[y] = smallest_available_color(y, adj, working, nv);
                    changed = true;
                }
            }
        }

        if (!changed) break;
    }

    for (uint32_t u = 0; u < nv; u++) {
        if (working[u] != master_colors[u]) {
            result.corrections.push_back({u, (uint32_t)working[u]});
        }
    }

    master_colors = working;
    return result;
}

/* ---- Ghost refresh ---- */

static vector<DeltaEntry> compute_ghost_refresh(
        const SubGraph &sg,
        const vector<int32_t> &master_colors) {
    vector<DeltaEntry> refresh;
    for (size_t i = 0; i < sg.ghost_nodes.size(); i++) {
        uint32_t gid = sg.ghost_nodes[i];
        refresh.push_back({gid, (uint32_t)master_colors[gid]});
    }
    return refresh;
}

/* ---- Filter corrections ---- */

static vector<DeltaEntry> filter_corrections(
        const vector<DeltaEntry> &corrections,
        const SubGraph &sg) {
    unordered_set<uint32_t> node_set;
    for (uint32_t gid : sg.local_to_global)
        node_set.insert(gid);

    vector<DeltaEntry> filtered;
    for (auto &c : corrections)
        if (node_set.count(c.node_id))
            filtered.push_back(c);
    return filtered;
}

/* ===========================================================================
 * MAIN
 * =========================================================================== */

int main(int argc, char *argv[]) {
    if (argc < 8) {
        cerr << "Usage: " << argv[0] << " <graph.mtx> <colors_file> <batch_dir> "
             << "<num_batches> <num_threads> <K> slave1:port [slave2:port ...]" << endl;
        return 1;
    }

    string input_file = argv[1];
    string colors_file = argv[2];
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
    cout << " DST_MST_SLV GC Pipeline - MASTER (DPU offload: ProcessCE + CheckConflict)" << endl;
    cout << "================================================================================" << endl;
    cout << "Dataset:       " << dataset_name << endl;
    cout << "Colors file:   " << colors_file << endl;
    cout << "Batch dir:     " << batch_dir << endl;
    cout << "Num batches:   " << num_batches << endl;
    cout << "Partitions:    " << K << endl;
    cout << "Threads:       " << num_threads << endl;
    cout << "DPU offload:   ProcessCE (P1) + CheckConflict (P2) on ARM" << endl;
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

    /* Load initial colors */
    vector<int32_t> colors(num_nodes, -1);
    {
        ifstream cf(colors_file);
        if (!cf.is_open()) {
            cerr << "ERROR: Cannot open colors file: " << colors_file << endl;
            return 1;
        }
        string line;
        uint32_t idx = 0;
        while (getline(cf, line)) {
            if (line.empty()) continue;
            if (idx < num_nodes) colors[idx++] = stoi(line);
        }
        cout << "[Master] Loaded colors for " << idx << " vertices" << endl;
    }

    int initial_chromatic = 0;
    for (uint32_t i = 0; i < num_nodes; i++)
        if (colors[i] + 1 > initial_chromatic) initial_chromatic = colors[i] + 1;
    cout << "[Master] Initial chromatic number: " << initial_chromatic << endl;

    vector<uint8_t> dummy_membership(num_nodes, 0);

    auto t1 = chrono::high_resolution_clock::now();
    double preprocess_ms = chrono::duration_cast<chrono::microseconds>(t1 - t0).count() / 1000.0;

    /* ================================================================
     * PHASE 2: PARTITIONING
     * ================================================================ */
    cout << "\n=== Phase 2: METIS Partitioning ===" << endl;
    auto t_part0 = chrono::high_resolution_clock::now();

    PartitionResult part_result = partition_graph(num_nodes, adj, K);

    vector<SubGraph> subgraphs(K);
    for (int p = 0; p < K; p++)
        subgraphs[p] = extract_subgraph(num_nodes, adj, dummy_membership, part_result.part, p);

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

        /* Send colors for this partition's local nodes */
        uint32_t local_nv = subgraphs[p].local_nv;
        vector<int32_t> local_colors(local_nv);
        for (uint32_t i = 0; i < local_nv; i++)
            local_colors[i] = colors[subgraphs[p].local_to_global[i]];
        send_all(slave_socks[p], local_colors.data(), local_nv * sizeof(int32_t));

        cout << "[Master] Sent partition " << p << " + colors (nv=" << local_nv << ")" << endl;
    }

    auto t_dist1 = chrono::high_resolution_clock::now();
    double dist_ms = chrono::duration_cast<chrono::microseconds>(t_dist1 - t_dist0).count() / 1000.0;

    /* Receive setup timings (5 doubles: recv_part, ghost_init, host_graph, connect_dpu, send_graph) */
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
     * PHASE 4: BATCH LOOP
     * ================================================================ */
    cout << "\n=== Phase 4: Batch Processing ===" << endl;

    int total_batch_files = countBatchFiles(batch_dir);
    vector<int> selected = selectBatches(total_batch_files, num_batches);
    cout << "[Master] Selected " << selected.size() << " batches" << endl;

    double total_wall_ms = 0;
    int batches_completed = 0;
    int total_corrections = 0;

    /* Priming batch per-partition */
    vector<double> prime_send_v(K, 0), prime_dpu_p1_v(K, 0), prime_dpu_p2_v(K, 0);
    vector<double> prime_recv_v(K, 0), prime_wall_v(K, 0);

    /* Steady-state per-partition accumulators */
    vector<double> ss_send_v(K, 0), ss_dpu_p1_v(K, 0), ss_dpu_p2_v(K, 0);
    vector<double> ss_recv_v(K, 0), ss_p3_v(K, 0), ss_wall_v(K, 0);
    int ss_count = 0;

    double total_conflict_us = 0, total_ghost_refresh_us = 0;

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

        /* Receive boundary deltas + timing from all slaves (6 doubles) */
        vector<vector<DeltaEntry>> all_deltas(K);
        vector<double> slave_wall(K), slave_dpu_p1(K), slave_dpu_p2(K);
        vector<double> slave_p3(K), slave_send(K), slave_recv(K);

        for (int p = 0; p < K; p++) {
            uint32_t num_deltas = recv_u32(slave_socks[p]);
            if (num_deltas > 0) {
                all_deltas[p].resize(num_deltas);
                recv_all(slave_socks[p], all_deltas[p].data(), num_deltas * sizeof(DeltaEntry));
            }
            slave_wall[p] = recv_double(slave_socks[p]);
            slave_dpu_p1[p] = recv_double(slave_socks[p]);
            slave_dpu_p2[p] = recv_double(slave_socks[p]);
            slave_p3[p] = recv_double(slave_socks[p]);
            slave_send[p] = recv_double(slave_socks[p]);
            slave_recv[p] = recv_double(slave_socks[p]);
        }

        /* Accumulate priming vs steady-state */
        if (round == 0) {
            for (int p = 0; p < K; p++) {
                prime_send_v[p] = slave_send[p];
                prime_dpu_p1_v[p] = slave_dpu_p1[p];
                prime_dpu_p2_v[p] = slave_dpu_p2[p];
                prime_recv_v[p] = slave_recv[p];
                prime_wall_v[p] = slave_wall[p];
            }
        } else {
            for (int p = 0; p < K; p++) {
                ss_send_v[p] += slave_send[p];
                ss_dpu_p1_v[p] += slave_dpu_p1[p];
                ss_dpu_p2_v[p] += slave_dpu_p2[p];
                ss_recv_v[p] += slave_recv[p];
                ss_p3_v[p] += slave_p3[p];
                ss_wall_v[p] += slave_wall[p];
            }
            ss_count++;
        }

        /* Color conflict resolution */
        auto t_cr0 = chrono::high_resolution_clock::now();
        auto cr_result = resolve_boundary_color_conflicts(
            all_deltas, colors, adj, num_nodes);
        auto t_cr1 = chrono::high_resolution_clock::now();
        total_conflict_us += chrono::duration_cast<chrono::microseconds>(t_cr1 - t_cr0).count();
        total_corrections += cr_result.corrections.size();

        /* Send corrections to each slave */
        for (int p = 0; p < K; p++) {
            auto filtered = filter_corrections(cr_result.corrections, subgraphs[p]);
            uint32_t nc = (uint32_t)filtered.size();
            send_u32(slave_socks[p], nc);
            if (nc > 0)
                send_all(slave_socks[p], filtered.data(), nc * sizeof(DeltaEntry));
        }

        /* Ghost refresh */
        auto t_gr0 = chrono::high_resolution_clock::now();
        for (int p = 0; p < K; p++) {
            uint32_t do_refresh = 1;
            send_u32(slave_socks[p], do_refresh);
            auto refresh = compute_ghost_refresh(subgraphs[p], colors);
            uint32_t nr = (uint32_t)refresh.size();
            send_u32(slave_socks[p], nr);
            if (nr > 0)
                send_all(slave_socks[p], refresh.data(), nr * sizeof(DeltaEntry));
        }
        auto t_gr1 = chrono::high_resolution_clock::now();
        total_ghost_refresh_us += chrono::duration_cast<chrono::microseconds>(t_gr1 - t_gr0).count();

        auto t_round1 = chrono::high_resolution_clock::now();
        double round_ms = chrono::duration_cast<chrono::microseconds>(t_round1 - t_round0).count() / 1000.0;
        total_wall_ms += round_ms;
        batches_completed++;

        cout << "[Batch " << (round+1) << "/" << selected.size() << "] wall=" << fixed << setprecision(3) << round_ms << "ms";
        for (int p = 0; p < K; p++)
            cout << " P" << p << "={p1=" << slave_dpu_p1[p]
                 << "ms,p2=" << slave_dpu_p2[p]
                 << "ms,p3=" << slave_p3[p] << "ms}";
        cout << " corrections=" << cr_result.corrections.size() << endl;
    }

    /* ================================================================
     * PHASE 5: SHUTDOWN
     * ================================================================ */
    cout << "\n=== Phase 5: Shutdown ===" << endl;

    for (int p = 0; p < K; p++)
        send_u32(slave_socks[p], SHUTDOWN_SENTINEL);

    for (int p = 0; p < K; p++) {
        auto final_colors_vec = recv_vec<DeltaEntry>(slave_socks[p]);
        for (auto &d : final_colors_vec)
            if (d.node_id < num_nodes)
                colors[d.node_id] = (int32_t)d.new_val;
        close(slave_socks[p]);
    }

    /* Verify coloring */
    int conflicts = 0;
    for (uint32_t u = 0; u < num_nodes; u++) {
        for (uint32_t v : adj[u]) {
            if (v > u && v < num_nodes && colors[u] == colors[v] && colors[u] >= 0)
                conflicts++;
        }
    }

    int final_chromatic = 0;
    for (uint32_t i = 0; i < num_nodes; i++)
        if (colors[i] + 1 > final_chromatic) final_chromatic = colors[i] + 1;

    auto t_total_end = chrono::high_resolution_clock::now();
    double total_ms = chrono::duration_cast<chrono::microseconds>(t_total_end - t_total_start).count() / 1000.0;

    cout << endl;
    cout << "================================================================================" << endl;
    cout << "             DST_MST_SLV GC PROFILING RESULTS" << endl;
    cout << "================================================================================" << endl;
    cout << "Initial chromatic:  " << initial_chromatic << endl;
    cout << "Final chromatic:    " << final_chromatic << endl;
    cout << "Color conflicts:    " << conflicts << endl;
    cout << "Total corrections:  " << total_corrections << endl;
    cout << "Total time:         " << fixed << setprecision(3) << total_ms << " ms" << endl;
    if (batches_completed > 0)
        cout << "Avg wall/batch:     " << total_wall_ms / batches_completed << " ms" << endl;
    cout << "================================================================================" << endl;

    /* Write CSV */
    string csv_path = "results.csv";
    bool csv_exists = ifstream(csv_path).good();
    ofstream csv(csv_path, ios::app);
    if (csv.is_open()) {
        if (!csv_exists) {
            csv << "Dataset,BatchRatio,InitChromatic,FinalChromatic,Conflicts,"
                << "Preprocess_ms,Partition_ms,Distribution_ms,"
                << "AvgDpuP1_P0,AvgDpuP2_P0,AvgP3_P0,AvgWall_P0,"
                << "AvgDpuP1_P1,AvgDpuP2_P1,AvgP3_P1,AvgWall_P1,"
                << "TotalBatch_ms,TotalTime_ms,TotalCorrections" << endl;
        }

        string batch_ratio = "unknown";
        {
            string bd = batch_dir;
            if (!bd.empty() && bd.back() == '/') bd.pop_back();
            size_t pos = bd.rfind('/');
            if (pos != string::npos) batch_ratio = bd.substr(pos + 1);
        }

        csv << dataset_name << "," << batch_ratio << ","
            << initial_chromatic << "," << final_chromatic << "," << conflicts << ","
            << fixed << setprecision(3)
            << preprocess_ms << "," << partition_ms << "," << dist_ms << ",";

        for (int p = 0; p < min(K, 2); p++) {
            if (ss_count > 0) {
                csv << ss_dpu_p1_v[p]/ss_count << ","
                    << ss_dpu_p2_v[p]/ss_count << ","
                    << ss_p3_v[p]/ss_count << ","
                    << ss_wall_v[p]/ss_count << ",";
            } else {
                csv << "0,0,0,0,";
            }
        }

        csv << total_wall_ms << "," << total_ms << "," << total_corrections << endl;
    }

    return 0;
}
