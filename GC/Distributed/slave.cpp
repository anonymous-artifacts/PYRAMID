/*
 * slave.cpp - Slave node for Distributed CPU-Only Graph Coloring Pipeline
 *
 * Runs on cn04/cn05 via SLURM. Listens for master connection.
 * Runs all 3 phases (ProcessCE + CheckConflict + UpdateNeighbors) locally on CPU.
 * No DPU offloading.
 *
 * Usage: ./slave <listen_port> [num_threads]
 */

#include <iostream>
#include <vector>
#include <algorithm>
#include <chrono>
#include <iomanip>
#include <cstring>
#include <unordered_map>
#include <unordered_set>
#include <cmath>
#include <unistd.h>
#include <omp.h>

#include "protocol.h"
#include "graph_utils.h"
#include "ghost_manager.h"

using namespace std;

#define MAX_COLORS 1024

/* ===========================================================================
 * SMALLEST AVAILABLE COLOR
 * =========================================================================== */

static int32_t smallest_available_color(uint32_t u, const HostGraph &graph,
                                         const vector<int32_t> &colors) {
    uint8_t used[MAX_COLORS];
    memset(used, 0, MAX_COLORS);
    for (uint32_t nb : graph.neighbors(u)) {
        if (nb < (uint32_t)colors.size() && colors[nb] >= 0 && colors[nb] < MAX_COLORS)
            used[colors[nb]] = 1;
    }
    for (int32_t c = 0; c < MAX_COLORS; c++)
        if (!used[c]) return c;
    return MAX_COLORS;
}

/* ===========================================================================
 * PHASE 1: ProcessCE (Deletion optimization + Insertion conflict detection)
 * =========================================================================== */

static vector<uint32_t> process_ce(
        const vector<RawEdge> &local_edges,
        HostGraph &graph,
        vector<int32_t> &colors,
        const GhostManager &ghost_mgr) {

    vector<uint32_t> affected;

    /* Separate insertions and deletions */
    vector<RawEdge> deletions, insertions;
    for (auto &e : local_edges) {
        if (e.src >= graph.nv || e.dst >= graph.nv || e.src == e.dst) continue;
        if (graph.is_adjacent(e.src, e.dst))
            deletions.push_back(e);
        else
            insertions.push_back(e);
    }

    /* Apply topology update FIRST (GC updates topology before coloring) */
    for (auto &e : deletions)
        graph.remove_edge(e.src, e.dst);
    for (auto &e : insertions)
        graph.insert_edge(e.src, e.dst);

    /* Process deletions: try to improve color */
    for (auto &e : deletions) {
        uint32_t a = e.src, b = e.dst;
        if (a >= graph.nv || b >= graph.nv) continue;

        int32_t ca = colors[a], cb = colors[b];
        uint32_t y = (ca >= cb) ? a : b;  /* higher-color node */
        uint32_t z = (y == a) ? b : a;
        int32_t target_color = colors[z];

        if (!ghost_mgr.is_owned(y)) continue;

        /* Check if target_color is still used by y's neighbors */
        bool in_SC = false;
        for (uint32_t nbr : graph.neighbors(y)) {
            if (nbr == z) continue;
            if (nbr < (uint32_t)colors.size() && colors[nbr] == target_color) {
                in_SC = true;
                break;
            }
        }
        if (!in_SC) {
            colors[y] = target_color;
            affected.push_back(y);
        }
    }

    /* Process insertions: detect and fix conflicts */
    for (auto &e : insertions) {
        uint32_t a = e.src, b = e.dst;
        if (a >= graph.nv || b >= graph.nv) continue;

        if (colors[a] == colors[b]) {
            uint32_t y = (a > b) ? a : b;  /* higher-ID node */
            if (!ghost_mgr.is_owned(y)) continue;
            colors[y] = smallest_available_color(y, graph, colors);
            affected.push_back(y);
        }
    }

    return affected;
}

/* ===========================================================================
 * PHASE 2: CheckConflict (iterative conflict resolution)
 * =========================================================================== */

static vector<uint32_t> check_conflict(
        uint32_t v, HostGraph &graph,
        vector<int32_t> &colors, vector<int32_t> &prev_colors,
        const GhostManager &ghost_mgr) {

    vector<uint32_t> newly_affected;
    int32_t cv = colors[v];

    for (uint32_t u : graph.neighbors(v)) {
        if (u >= (uint32_t)colors.size()) continue;
        if (colors[u] == cv) {
            uint32_t y = (u > v) ? u : v;
            if (!ghost_mgr.is_owned(y)) continue;
            prev_colors[y] = colors[y];
            colors[y] = smallest_available_color(y, graph, colors);
            newly_affected.push_back(y);
        }
    }
    return newly_affected;
}

/* ===========================================================================
 * PHASE 3: UpdateNeighbors (propagate freed colors)
 * =========================================================================== */

static vector<uint32_t> update_neighbors(
        uint32_t v, HostGraph &graph,
        vector<int32_t> &colors, vector<int32_t> &prev_colors,
        const GhostManager &ghost_mgr) {

    vector<uint32_t> newly_affected;
    int32_t freed_color = prev_colors[v];
    if (freed_color < 0) return newly_affected;

    for (uint32_t u : graph.neighbors(v)) {
        if (u >= (uint32_t)colors.size()) continue;
        if (!ghost_mgr.is_owned(u)) continue;

        if (freed_color < colors[u]) {
            /* Check if freed_color is still available for u */
            bool in_SC = false;
            for (uint32_t nbr : graph.neighbors(u)) {
                if (nbr < (uint32_t)colors.size() && colors[nbr] == freed_color) {
                    in_SC = true;
                    break;
                }
            }
            if (!in_SC) {
                prev_colors[u] = colors[u];
                colors[u] = freed_color;
                newly_affected.push_back(u);
            }
        }
    }
    return newly_affected;
}

/* ===========================================================================
 * TRANSLATE GLOBAL EDGE IDS → LOCAL IDS
 * =========================================================================== */

static vector<RawEdge> translate_to_local(const vector<RawEdge> &global_edges,
                                            const GhostManager &ghost_mgr) {
    vector<RawEdge> local_edges;
    local_edges.reserve(global_edges.size());
    for (auto &e : global_edges) {
        uint32_t ls = ghost_mgr.to_local(e.src);
        uint32_t ld = ghost_mgr.to_local(e.dst);
        if (ls != UINT32_MAX && ld != UINT32_MAX)
            local_edges.push_back({ls, ld});
    }
    return local_edges;
}

/* ===========================================================================
 * MAIN
 * =========================================================================== */

int main(int argc, char *argv[]) {
    if (argc < 2) {
        cerr << "Usage: " << argv[0] << " <listen_port> [num_threads]" << endl;
        return 1;
    }

    int listen_port = atoi(argv[1]);
    int n_threads = (argc > 2) ? atoi(argv[2]) : 32;
    omp_set_num_threads(n_threads);

    char hostname[256];
    gethostname(hostname, sizeof(hostname));
    cout << "[Slave " << hostname << "] Starting on port " << listen_port
         << " (threads=" << n_threads << ")" << endl;

    /* ================================================================
     * PHASE 0: WAIT FOR MASTER CONNECTION
     * ================================================================ */
    int server_sock = create_server(listen_port);
    cout << "[Slave " << hostname << "] READY on port " << listen_port << endl;
    cout.flush();

    /* Loop to handle multiple master connections (one per experiment) */
    while (true) {

    int master_sock = accept_one(server_sock);
    cout << "[Slave " << hostname << "] Master connected" << endl;

    /* ================================================================
     * PHASE 1: RECEIVE PARTITION FROM MASTER
     * ================================================================ */
    auto t_rp0 = chrono::high_resolution_clock::now();
    PartitionMeta meta;
    SubGraph sg = recv_partition(master_sock, meta);
    auto t_rp1 = chrono::high_resolution_clock::now();
    double recv_part_ms = chrono::duration_cast<chrono::microseconds>(t_rp1 - t_rp0).count() / 1000.0;

    cout << "[Slave " << hostname << "] Partition " << meta.partition_id
         << ": local_nv=" << meta.local_nv
         << " local_ne=" << meta.local_ne
         << " owned=" << meta.owned_count
         << " ghost=" << meta.num_ghost << endl;

    /* Receive local colors from master */
    vector<int32_t> colors(meta.local_nv, -1);
    recv_all(master_sock, colors.data(), meta.local_nv * sizeof(int32_t));
    vector<int32_t> prev_colors(meta.local_nv, -1);
    for (uint32_t i = 0; i < meta.local_nv; i++)
        prev_colors[i] = colors[i];

    cout << "[Slave " << hostname << "] Received colors for " << meta.local_nv << " local vertices" << endl;

    auto t_gi0 = chrono::high_resolution_clock::now();
    vector<uint32_t> local_to_global = sg.local_to_global;

    GhostManager ghost_mgr;
    ghost_mgr.init(meta.owned_count, local_to_global, sg.ghost_nodes,
                   sg.ghost_owner, sg.boundary_nodes);
    auto t_gi1 = chrono::high_resolution_clock::now();
    double ghost_init_ms = chrono::duration_cast<chrono::microseconds>(t_gi1 - t_gi0).count() / 1000.0;

    /* ================================================================
     * PHASE 2: BUILD HOST GRAPH (no DPU connection)
     * ================================================================ */
    auto t_hg0 = chrono::high_resolution_clock::now();
    HostGraph host_graph;
    host_graph.build_from_csr(meta.local_nv, sg.csr_offsets, sg.csr_nbrs);
    auto t_hg1 = chrono::high_resolution_clock::now();
    double host_graph_ms = chrono::duration_cast<chrono::microseconds>(t_hg1 - t_hg0).count() / 1000.0;
    cout << "[Slave " << hostname << "] HostGraph built (" << fixed << setprecision(1) << host_graph_ms << " ms)" << endl;

    /* Free CSR copies */
    sg.csr_offsets.clear(); sg.csr_offsets.shrink_to_fit();
    sg.csr_nbrs.clear(); sg.csr_nbrs.shrink_to_fit();

    /* Send setup timings to master (3 doubles — no DPU) */
    send_double(master_sock, recv_part_ms);
    send_double(master_sock, ghost_init_ms);
    send_double(master_sock, host_graph_ms);

    cout << "[Slave " << hostname << "] Setup: recv_part=" << fixed << setprecision(1) << recv_part_ms
         << "ms ghost_init=" << ghost_init_ms
         << "ms host_graph=" << host_graph_ms << "ms" << endl;

    /* ================================================================
     * PHASE 3: BATCH LOOP (sequential — no overlapped pipeline)
     * ================================================================ */
    int round = 0;

    while (true) {
        uint32_t num_edges = recv_u32(master_sock);

        if (num_edges == SHUTDOWN_SENTINEL) {
            cout << "[Slave " << hostname << "] Shutdown received" << endl;
            break;
        }

        vector<RawEdge> global_edges(num_edges);
        if (num_edges > 0)
            recv_all(master_sock, global_edges.data(), num_edges * sizeof(RawEdge));

        vector<RawEdge> local_edges = translate_to_local(global_edges, ghost_mgr);

        auto t_iter_start = chrono::high_resolution_clock::now();

        /* Phase 1: ProcessCE (CPU) — topology update + initial conflict detection */
        auto t_p1_start = chrono::high_resolution_clock::now();
        auto affected = process_ce(local_edges, host_graph, colors, ghost_mgr);
        auto t_p1_end = chrono::high_resolution_clock::now();
        double p1_ms = chrono::duration_cast<chrono::microseconds>(t_p1_end - t_p1_start).count() / 1000.0;

        /* Set prev_colors for affected nodes */
        for (uint32_t v : affected)
            prev_colors[v] = colors[v];

        /* Phases 2+3: Iterative convergence loop */
        double p2_ms = 0, p3_ms = 0;

        sort(affected.begin(), affected.end());
        affected.erase(unique(affected.begin(), affected.end()), affected.end());

        int itr = 0;
        while (!affected.empty() && itr < 50) {
            vector<uint32_t> S = affected;
            affected.clear();

            /* Phase 2: CheckConflict */
            auto t2 = chrono::high_resolution_clock::now();
            for (uint32_t v : S) {
                auto A1 = check_conflict(v, host_graph, colors, prev_colors, ghost_mgr);
                affected.insert(affected.end(), A1.begin(), A1.end());
            }
            p2_ms += chrono::duration_cast<chrono::microseconds>(
                chrono::high_resolution_clock::now() - t2).count() / 1000.0;

            /* Phase 3: UpdateNeighbors */
            auto t3 = chrono::high_resolution_clock::now();
            {
                vector<uint32_t> next_aff;
                for (uint32_t v : affected) {
                    auto A2 = update_neighbors(v, host_graph, colors, prev_colors, ghost_mgr);
                    next_aff.insert(next_aff.end(), A2.begin(), A2.end());
                }
                affected = next_aff;
            }
            p3_ms += chrono::duration_cast<chrono::microseconds>(
                chrono::high_resolution_clock::now() - t3).count() / 1000.0;

            sort(affected.begin(), affected.end());
            affected.erase(unique(affected.begin(), affected.end()), affected.end());
            itr++;
        }

        auto t_iter_end = chrono::high_resolution_clock::now();
        double wall_ms = chrono::duration_cast<chrono::microseconds>(t_iter_end - t_iter_start).count() / 1000.0;

        /* Collect all changed owned nodes for boundary deltas */
        vector<uint32_t> all_changed;
        for (uint32_t i = 0; i < meta.owned_count; i++) {
            if (colors[i] != prev_colors[i])
                all_changed.push_back(i);
        }

        auto boundary_deltas = ghost_mgr.collect_boundary_color_deltas(all_changed, colors);
        uint32_t nd = (uint32_t)boundary_deltas.size();
        send_u32(master_sock, nd);
        if (nd > 0)
            send_all(master_sock, boundary_deltas.data(), nd * sizeof(DeltaEntry));

        /* Send timing feedback (4 doubles: wall, p1, p2, p3) */
        send_double(master_sock, wall_ms);
        send_double(master_sock, p1_ms);
        send_double(master_sock, p2_ms);
        send_double(master_sock, p3_ms);

        /* Receive corrections from master */
        uint32_t nc = recv_u32(master_sock);
        if (nc > 0) {
            vector<DeltaEntry> corrections(nc);
            recv_all(master_sock, corrections.data(), nc * sizeof(DeltaEntry));
            ghost_mgr.apply_color_corrections(corrections, colors);
        }

        /* Ghost refresh */
        uint32_t do_refresh = recv_u32(master_sock);
        if (do_refresh) {
            uint32_t nr = recv_u32(master_sock);
            if (nr > 0) {
                vector<DeltaEntry> refresh(nr);
                recv_all(master_sock, refresh.data(), nr * sizeof(DeltaEntry));
                ghost_mgr.apply_ghost_color_refresh(refresh, colors);
            }
        }

        /* Update prev_colors for next round */
        for (uint32_t i = 0; i < meta.local_nv; i++)
            prev_colors[i] = colors[i];

        cout << "[Slave " << hostname << " R" << round << "] "
             << "P1=" << fixed << setprecision(3) << p1_ms
             << "ms P2=" << p2_ms
             << "ms P3=" << p3_ms
             << "ms wall=" << wall_ms << "ms"
             << " itr=" << itr
             << " deltas=" << nd << endl;

        round++;
    }

    /* Send final owned colors to master */
    vector<DeltaEntry> final_colors;
    for (uint32_t i = 0; i < meta.owned_count; i++)
        final_colors.push_back({local_to_global[i], (uint32_t)colors[i]});
    send_vec(master_sock, final_colors);

    close(master_sock);

    cout << "[Slave " << hostname << "] Done. " << round << " rounds. Waiting for next connection..." << endl;
    cout.flush();

    } /* end while(true) — loop back to accept next master connection */

    close(server_sock);
    return 0;
}
