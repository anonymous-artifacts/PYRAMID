/*
 * master.cpp - Master node for Distributed CPU-Only MIS Pipeline
 *
 * Runs on login node. Connects to slave processes on cn04/cn05 via TCP.
 * Handles graph loading, METIS partitioning, batch distribution,
 * boundary conflict resolution, and ghost refresh.
 *
 * No DPU — slaves run all 3 phases (BFS + Clustering + MIS update) on CPU.
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

/* ---- Batch file reading (pyramid convention) ---- */

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

/* ---- Iterative conflict resolution for cross-partition boundaries ---- */

struct ConflictResolutionResult {
    vector<BoundaryDelta> corrections;
    int iterations;
};

static ConflictResolutionResult resolve_boundary_conflicts_iterative(
        const vector<vector<BoundaryDelta>> &all_deltas,
        vector<uint8_t> &master_membership,
        const vector<vector<uint32_t>> &adj,
        uint32_t nv) {

    ConflictResolutionResult result;
    result.iterations = 0;

    vector<uint8_t> working(master_membership.begin(), master_membership.end());
    for (auto &partition_deltas : all_deltas)
        for (auto &d : partition_deltas)
            if (d.global_node_id < nv)
                working[d.global_node_id] = d.new_membership;

    const int MAX_ITERATIONS = 50;

    while (result.iterations < MAX_ITERATIONS) {
        result.iterations++;
        bool changed = false;

        /* Pass 1: Remove independence violations */
        vector<uint32_t> to_remove;
        for (uint32_t u = 0; u < nv; u++) {
            if (!working[u]) continue;
            for (uint32_t v : adj[u]) {
                if (v <= u || v >= nv) continue;
                if (working[v]) {
                    to_remove.push_back(u);
                    break;
                }
            }
        }

        if (to_remove.empty()) {
            /* Maximality pass */
            vector<uint32_t> to_add;
            for (uint32_t u = 0; u < nv; u++) {
                if (working[u]) continue;
                bool has_mis_nb = false;
                for (uint32_t v : adj[u]) {
                    if (v < nv && working[v]) { has_mis_nb = true; break; }
                }
                if (!has_mis_nb) to_add.push_back(u);
            }

            for (uint32_t node : to_add) {
                bool ok = true;
                for (uint32_t nb : adj[node]) {
                    if (nb < nv && working[nb]) { ok = false; break; }
                }
                if (ok) {
                    working[node] = 1;
                    changed = true;
                }
            }

            if (!changed) break;
        } else {
            sort(to_remove.begin(), to_remove.end());
            to_remove.erase(unique(to_remove.begin(), to_remove.end()), to_remove.end());
            for (uint32_t node : to_remove)
                working[node] = 0;
            changed = true;
        }
    }

    for (uint32_t u = 0; u < nv; u++) {
        if (working[u] != master_membership[u]) {
            BoundaryDelta fix;
            fix.global_node_id = u;
            fix.new_membership = working[u];
            fix.pad[0] = fix.pad[1] = fix.pad[2] = 0;
            result.corrections.push_back(fix);
        }
    }

    memcpy(master_membership.data(), working.data(), nv);
    return result;
}

/* ---- Ghost refresh for a partition ---- */

static vector<DeltaEntry> compute_ghost_refresh(
        const SubGraph &sg,
        const vector<uint8_t> &master_membership) {
    vector<DeltaEntry> refresh;
    for (size_t i = 0; i < sg.ghost_nodes.size(); i++) {
        uint32_t gid = sg.ghost_nodes[i];
        refresh.push_back({gid, (uint32_t)master_membership[gid]});
    }
    return refresh;
}

/* ---- Filter corrections relevant to a partition ---- */

static vector<BoundaryDelta> filter_corrections(
        const vector<BoundaryDelta> &corrections,
        const SubGraph &sg) {
    unordered_set<uint32_t> node_set;
    for (uint32_t gid : sg.local_to_global)
        node_set.insert(gid);

    vector<BoundaryDelta> filtered;
    for (auto &c : corrections)
        if (node_set.count(c.global_node_id))
            filtered.push_back(c);
    return filtered;
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
        cerr << "ERROR: Need " << K << " slave addresses, got " << slave_addrs.size() << endl;
        return 1;
    }

    string dataset_name = extract_dataset_name(input_file);

    cout << "================================================================================" << endl;
    cout << " Distributed CPU-Only MIS Pipeline - MASTER" << endl;
    cout << "================================================================================" << endl;
    cout << "Dataset:       " << dataset_name << endl;
    cout << "MIS file:      " << mis_file << endl;
    cout << "Batch dir:     " << batch_dir << endl;
    cout << "Num batches:   " << num_batches << endl;
    cout << "Partitions:    " << K << endl;
    cout << "Threads:       " << num_threads << endl;
    cout << "Slaves:        ";
    for (int i = 0; i < K; i++) cout << slave_addrs[i] << (i < K-1 ? ", " : "");
    cout << endl;
    cout << "================================================================================" << endl << endl;

    auto t_total_start = chrono::high_resolution_clock::now();

    /* ================================================================
     * PHASE 1: PREPROCESSING
     * ================================================================ */
    cout << "=== Phase 1: Preprocessing ===" << endl;
    auto t0 = chrono::high_resolution_clock::now();

    auto t_read0 = chrono::high_resolution_clock::now();
    EdgeListData raw = read_raw_edges(input_file);
    uint32_t num_nodes = raw.num_nodes;
    auto t_read1 = chrono::high_resolution_clock::now();
    double read_ms = chrono::duration_cast<chrono::microseconds>(t_read1 - t_read0).count() / 1000.0;

    auto t_back0 = chrono::high_resolution_clock::now();
    add_backedges(raw.edges);
    auto t_back1 = chrono::high_resolution_clock::now();
    double backedge_ms = chrono::duration_cast<chrono::microseconds>(t_back1 - t_back0).count() / 1000.0;

    auto t_adj0 = chrono::high_resolution_clock::now();
    vector<vector<uint32_t>> adj;
    build_sorted_adj(num_nodes, raw.edges, adj);
    raw.edges.clear();
    auto t_adj1 = chrono::high_resolution_clock::now();
    double adj_build_ms = chrono::duration_cast<chrono::microseconds>(t_adj1 - t_adj0).count() / 1000.0;

    /* Load initial MIS from preprocessed file */
    auto t_mis0 = chrono::high_resolution_clock::now();
    vector<uint8_t> membership(num_nodes, 0);
    int initial_card = 0;
    {
        ifstream mf(mis_file);
        if (!mf.is_open()) {
            cerr << "ERROR: Cannot open MIS file: " << mis_file << endl;
            return 1;
        }
        string line;
        while (getline(mf, line)) {
            if (line.empty()) continue;
            int v = stoi(line);
            if (v + 1 >= 0 && (uint32_t)(v + 1) < num_nodes) {
                membership[v + 1] = 1;
                initial_card++;
            }
        }
    }
    auto t_mis1 = chrono::high_resolution_clock::now();
    double mis_load_ms = chrono::duration_cast<chrono::microseconds>(t_mis1 - t_mis0).count() / 1000.0;
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
     * PHASE 3: CONNECT TO SLAVES + DISTRIBUTE PARTITIONS
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
                    cerr << "[Master] ERROR: Cannot connect to slave " << p
                         << " at " << slave_addrs[p] << ": " << e.what() << endl;
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
    cout << "[Master] Distribution: " << fixed << setprecision(1) << dist_ms << " ms" << endl;

    /* Receive slave setup timings (3 doubles: recv_part, ghost_init, host_graph) */
    vector<double> slave_recv_part(K), slave_ghost_init(K), slave_host_graph(K);
    for (int p = 0; p < K; p++) {
        slave_recv_part[p] = recv_double(slave_socks[p]);
        slave_ghost_init[p] = recv_double(slave_socks[p]);
        slave_host_graph[p] = recv_double(slave_socks[p]);
    }
    for (int p = 0; p < K; p++)
        cout << "[Master] P" << p << " setup: recv_part=" << fixed << setprecision(1)
             << slave_recv_part[p] << "ms ghost_init=" << slave_ghost_init[p]
             << "ms host_graph=" << slave_host_graph[p] << "ms" << endl;

    /* ================================================================
     * PHASE 4: BATCH LOOP (preprocessed batch files)
     * ================================================================ */
    cout << "\n=== Phase 4: Batch Processing ===" << endl;

    /* Select batches */
    int total_batch_files = countBatchFiles(batch_dir);
    vector<int> selected = selectBatches(total_batch_files, num_batches);
    cout << "[Master] Total batch files: " << total_batch_files << endl;
    cout << "[Master] Selected: [";
    for (size_t i = 0; i < selected.size(); i++) {
        if (i > 0) cout << ", ";
        cout << selected[i];
    }
    cout << "]" << endl;

    double total_wall_ms = 0;
    int batches_completed = 0;
    int total_corrections = 0;

    /* Per-batch timing accumulators */
    double total_conflict_us = 0, total_ghost_refresh_us = 0;
    vector<double> ss_bfs_v(K, 0), ss_clust_v(K, 0), ss_mis_v(K, 0), ss_wall_v(K, 0);

    for (int round = 0; round < (int)selected.size(); round++) {
        int batch_idx = selected[round];
        string batch_path = batch_dir + "/" + to_string(batch_idx) + ".mtx";

        auto t_round0 = chrono::high_resolution_clock::now();

        /* Read batch file */
        vector<RawEdge> all_edges = readBatchFile(batch_path);
        if (all_edges.empty()) {
            cout << "[Batch " << (round+1) << "] SKIP: empty batch" << endl;
            continue;
        }

        /* Partition edges: send to partition(s) that own at least one endpoint */
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
                /* Check if insertion or deletion */
                bool is_adj = binary_search(adj[e.src].begin(), adj[e.src].end(), e.dst);
                if (!is_adj) {
                    /* Insert */
                    auto it = lower_bound(adj[e.src].begin(), adj[e.src].end(), e.dst);
                    adj[e.src].insert(it, e.dst);
                    it = lower_bound(adj[e.dst].begin(), adj[e.dst].end(), e.src);
                    adj[e.dst].insert(it, e.src);
                } else {
                    /* Delete */
                    auto it = lower_bound(adj[e.src].begin(), adj[e.src].end(), e.dst);
                    if (it != adj[e.src].end() && *it == e.dst) adj[e.src].erase(it);
                    it = lower_bound(adj[e.dst].begin(), adj[e.dst].end(), e.src);
                    if (it != adj[e.dst].end() && *it == e.src) adj[e.dst].erase(it);
                }
            }
        }

        /* Receive boundary deltas + timing from all slaves (4 doubles each) */
        vector<vector<BoundaryDelta>> all_deltas(K);
        vector<double> slave_bfs(K), slave_clust(K), slave_mis(K), slave_wall(K);

        for (int p = 0; p < K; p++) {
            uint32_t num_deltas = recv_u32(slave_socks[p]);
            if (num_deltas > 0) {
                all_deltas[p].resize(num_deltas);
                recv_all(slave_socks[p], all_deltas[p].data(), num_deltas * sizeof(BoundaryDelta));
            }
            slave_wall[p] = recv_double(slave_socks[p]);
            slave_bfs[p] = recv_double(slave_socks[p]);
            slave_clust[p] = recv_double(slave_socks[p]);
            slave_mis[p] = recv_double(slave_socks[p]);
        }

        /* Accumulate per-partition */
        for (int p = 0; p < K; p++) {
            ss_bfs_v[p] += slave_bfs[p];
            ss_clust_v[p] += slave_clust[p];
            ss_mis_v[p] += slave_mis[p];
            ss_wall_v[p] += slave_wall[p];
        }

        /* Conflict resolution */
        auto t_cr0 = chrono::high_resolution_clock::now();
        auto cr_result = resolve_boundary_conflicts_iterative(
            all_deltas, membership, adj, num_nodes);
        auto t_cr1 = chrono::high_resolution_clock::now();
        total_conflict_us += chrono::duration_cast<chrono::microseconds>(t_cr1 - t_cr0).count();
        total_corrections += cr_result.corrections.size();

        /* Send corrections to each slave */
        for (int p = 0; p < K; p++) {
            auto filtered = filter_corrections(cr_result.corrections, subgraphs[p]);
            uint32_t nc = (uint32_t)filtered.size();
            send_u32(slave_socks[p], nc);
            if (nc > 0)
                send_all(slave_socks[p], filtered.data(), nc * sizeof(BoundaryDelta));
        }

        /* Ghost refresh */
        auto t_gr0 = chrono::high_resolution_clock::now();
        for (int p = 0; p < K; p++) {
            uint32_t do_refresh = 1;
            send_u32(slave_socks[p], do_refresh);
            auto refresh = compute_ghost_refresh(subgraphs[p], membership);
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

        cout << "[Batch " << (round + 1) << "/" << selected.size() << "] wall=" << fixed << setprecision(3) << round_ms << "ms";
        for (int p = 0; p < K; p++)
            cout << " P" << p << "={bfs=" << slave_bfs[p]
                 << "ms,clust=" << slave_clust[p]
                 << "ms,mis=" << slave_mis[p] << "ms}";
        cout << " corrections=" << cr_result.corrections.size()
             << " iters=" << cr_result.iterations << endl;
    }

    /* ================================================================
     * PHASE 5: SHUTDOWN & FINAL
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

    /* Final MIS repair pass */
    int repair_removed = 0, repair_added = 0;

    for (uint32_t u = 0; u < num_nodes; u++) {
        if (!membership[u]) continue;
        for (uint32_t v : adj[u]) {
            if (v != u && v > u && v < num_nodes && membership[v]) {
                membership[u] = 0;
                repair_removed++;
                break;
            }
        }
    }

    for (uint32_t u = 0; u < num_nodes; u++) {
        if (membership[u]) continue;
        bool has_mis_nb = false;
        for (uint32_t v : adj[u]) {
            if (v != u && v < num_nodes && membership[v]) { has_mis_nb = true; break; }
        }
        if (!has_mis_nb) {
            bool conflict = false;
            for (uint32_t v : adj[u]) {
                if (v != u && v < num_nodes && membership[v]) { conflict = true; break; }
            }
            if (!conflict) { membership[u] = 1; repair_added++; }
        }
    }

    if (repair_removed > 0 || repair_added > 0)
        cout << "[Master] MIS repair: removed=" << repair_removed
             << " added=" << repair_added << endl;

    int final_card = 0;
    for (uint32_t i = 0; i < num_nodes; i++) if (membership[i]) final_card++;

    bool mis_valid = verify_mis(adj, membership, num_nodes);

    auto t_total_end = chrono::high_resolution_clock::now();
    double total_ms = chrono::duration_cast<chrono::microseconds>(t_total_end - t_total_start).count() / 1000.0;

    cout << endl;
    cout << "================================================================================" << endl;
    cout << "             DISTRIBUTED CPU-ONLY MIS PROFILING RESULTS" << endl;
    cout << "================================================================================" << endl;
    cout << endl;
    cout << "Initial MIS cardinality: " << initial_card << endl;
    cout << "Final MIS cardinality:   " << final_card << endl;
    cout << "MIS valid:               " << (mis_valid ? "YES" : "NO") << endl;
    cout << endl;

    cout << "  Phase 1: Preprocessing (Master)" << endl;
    cout << "  [A]  Read edge list            | " << fixed << setprecision(3) << read_ms << " ms" << endl;
    cout << "  [B]  Add backedges             | " << backedge_ms << " ms" << endl;
    cout << "  [C]  Build adjacency           | " << adj_build_ms << " ms" << endl;
    cout << "  [D]  Load initial MIS          | " << mis_load_ms << " ms" << endl;
    cout << endl;
    cout << "  Phase 2: Partitioning          | " << partition_ms << " ms" << endl;
    cout << "  Phase 3: Distribution          | " << dist_ms << " ms" << endl;
    cout << endl;

    for (int p = 0; p < K; p++) {
        cout << "  Slave P" << p << " Setup:" << endl;
        cout << "  [F]  Recv partition            | " << slave_recv_part[p] << " ms" << endl;
        cout << "  [G]  Ghost manager init        | " << slave_ghost_init[p] << " ms" << endl;
        cout << "  [H]  Build HostGraph           | " << slave_host_graph[p] << " ms" << endl;
        cout << endl;
    }

    if (batches_completed > 0) {
        for (int p = 0; p < K; p++) {
            cout << "  Avg Per-Batch P" << p << " (N=" << batches_completed << "):" << endl;
            cout << "  1.  CPU BFS (2-hop)          | " << ss_bfs_v[p] / batches_completed << " ms" << endl;
            cout << "  2.  CPU Clustering           | " << ss_clust_v[p] / batches_completed << " ms" << endl;
            cout << "  3.  CPU MIS Update           | " << ss_mis_v[p] / batches_completed << " ms" << endl;
            cout << "  4.  Wall time                | " << ss_wall_v[p] / batches_completed << " ms" << endl;
            cout << endl;
        }

        cout << "  Master Batch Sub-steps (avg per batch):" << endl;
        cout << "  1.  Conflict resolution      | " << total_conflict_us / batches_completed / 1000.0 << " ms" << endl;
        cout << "  2.  Ghost refresh            | " << total_ghost_refresh_us / batches_completed / 1000.0 << " ms" << endl;
        cout << endl;
        cout << "  Avg wall/batch: " << total_wall_ms / batches_completed << " ms" << endl;
        cout << "  Total batch:    " << total_wall_ms << " ms" << endl;
    }
    cout << "  Total time:     " << total_ms << " ms" << endl;
    cout << "  Corrections:    " << total_corrections << " total" << endl;
    cout << "================================================================================" << endl;

    /* ---- Write CSV ---- */
    string csv_path = "results.csv";
    bool csv_exists = ifstream(csv_path).good();
    ofstream csv(csv_path, ios::app);
    if (csv.is_open()) {
        if (!csv_exists) {
            csv << "Dataset,BatchRatio,InitialCard,FinalCard,MIS_Valid,"
                << "Preprocess_ms,Partition_ms,Distribution_ms,"
                << "AvgBFS_P0,AvgClust_P0,AvgMIS_P0,AvgWall_P0,"
                << "AvgBFS_P1,AvgClust_P1,AvgMIS_P1,AvgWall_P1,"
                << "AvgConflict_ms,AvgGhostRefresh_ms,"
                << "TotalBatch_ms,TotalTime_ms,TotalCorrections" << endl;
        }

        /* Extract batch ratio from batch_dir */
        string batch_ratio = "unknown";
        {
            string bd = batch_dir;
            if (!bd.empty() && bd.back() == '/') bd.pop_back();
            size_t pos = bd.rfind('/');
            if (pos != string::npos) batch_ratio = bd.substr(pos + 1);
        }

        csv << dataset_name << "," << batch_ratio << ","
            << initial_card << "," << final_card << "," << (mis_valid ? "YES" : "NO") << ","
            << fixed << setprecision(3)
            << preprocess_ms << "," << partition_ms << "," << dist_ms << ",";

        for (int p = 0; p < min(K, 2); p++) {
            if (batches_completed > 0) {
                csv << ss_bfs_v[p]/batches_completed << ","
                    << ss_clust_v[p]/batches_completed << ","
                    << ss_mis_v[p]/batches_completed << ","
                    << ss_wall_v[p]/batches_completed << ",";
            } else {
                csv << "0,0,0,0,";
            }
        }

        if (batches_completed > 0) {
            csv << total_conflict_us/batches_completed/1000.0 << ","
                << total_ghost_refresh_us/batches_completed/1000.0 << ",";
        } else {
            csv << "0,0,";
        }

        csv << total_wall_ms << "," << total_ms << "," << total_corrections << endl;
        csv.close();
    }

    return 0;
}
