/*
 * slave.cpp - Slave node for DST_MST_SLV_NOSYNC MIS Pipeline
 *
 * NO-SYNC variant: does local MIS update and sends deltas to master.
 * No corrections received, no ghost refresh — ghosts may become stale.
 *
 * Usage: ./slave <listen_port> <dpu_host:dpu_port>
 */

#include <iostream>
#include <vector>
#include <algorithm>
#include <chrono>
#include <iomanip>
#include <cstring>
#include <thread>
#include <unordered_map>
#include <unistd.h>

#include "protocol.h"
#include "graph_utils.h"
#include "ghost_manager.h"

using namespace std;

/* ===========================================================================
 * OWNERSHIP-AWARE MIS UPDATE FROM DPU CLUSTER ASSIGNMENTS
 * =========================================================================== */

struct OwnedMisResult {
    GraphSyncPayload sync;
    vector<uint32_t> changed_local_ids;
};

static OwnedMisResult cpu_mis_update_from_clusters(
        const DpuClusterResponse &clust,
        const vector<RawEdge> &batch_edges,
        HostGraph &graph,
        vector<uint8_t> &membership,
        const GhostManager &ghost_mgr) {

    OwnedMisResult result;
    if (!clust.ok || clust.is_ins.empty()) return result;

    uint32_t ne = clust.is_ins.size();

    /* Group edges by cluster_id */
    vector<vector<uint32_t>> cluster_edges(clust.num_clusters);
    for (uint32_t i = 0; i < ne; i++) {
        uint32_t cid = clust.cluster_ids[i];
        if (cid < clust.num_clusters)
            cluster_edges[cid].push_back(i);
    }

    vector<uint32_t> changed;
    changed.reserve(1024);

    for (uint32_t c = 0; c < clust.num_clusters; c++) {
        if (cluster_edges[c].empty()) continue;

        for (uint32_t ei : cluster_edges[c]) {
            uint32_t src = batch_edges[ei].src;
            uint32_t dst = batch_edges[ei].dst;
            if (src >= graph.nv || dst >= graph.nv) continue;

            if (src == dst) continue;

            if (clust.is_ins[ei]) {
                /* Edge insertion */
                if (membership[src] && membership[dst]) {
                    uint32_t rem = (src < dst) ? src : dst;
                    if (ghost_mgr.is_owned(rem)) {
                        membership[rem] = 0;
                        changed.push_back(rem);
                        for (uint32_t nb : graph.neighbors(rem)) {
                            if (nb == rem) continue;
                            if (!membership[nb] && ghost_mgr.is_owned(nb)) {
                                bool ok = true;
                                for (uint32_t nb2 : graph.neighbors(nb))
                                    if (nb2 != nb && membership[nb2]) { ok = false; break; }
                                if (ok) { membership[nb] = 1; changed.push_back(nb); }
                            }
                        }
                    }
                }
                graph.insert_edge(src, dst);
                result.sync.edge_mutations.push_back({src, dst, 1});
            } else {
                /* Edge deletion */
                if (membership[src] && !membership[dst] && ghost_mgr.is_owned(dst)) {
                    bool ok = true;
                    for (uint32_t nb : graph.neighbors(dst))
                        if (nb != dst && nb != src && membership[nb]) { ok = false; break; }
                    if (ok) { membership[dst] = 1; changed.push_back(dst); }
                } else if (!membership[src] && membership[dst] && ghost_mgr.is_owned(src)) {
                    bool ok = true;
                    for (uint32_t nb : graph.neighbors(src))
                        if (nb != src && nb != dst && membership[nb]) { ok = false; break; }
                    if (ok) { membership[src] = 1; changed.push_back(src); }
                }
                graph.remove_edge(src, dst);
                result.sync.edge_mutations.push_back({src, dst, 0});
            }
        }
    }

    sort(changed.begin(), changed.end());
    changed.erase(unique(changed.begin(), changed.end()), changed.end());
    result.changed_local_ids = changed;
    for (uint32_t node : changed)
        result.sync.membership_changes.push_back({node, (uint32_t)membership[node]});

    return result;
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
 * NOSYNC: Send deltas + timing only (no corrections or ghost refresh)
 * =========================================================================== */

static void tcp_send_deltas_nosync(
        int master_sock,
        const vector<uint32_t> &changed_local_ids,
        const vector<uint8_t> &membership,
        const GhostManager &ghost_mgr,
        double wall_ms, double dpu_bfs_ms, double dpu_clust_ms,
        double mis_ms, double send_ms, double recv_ms) {

    /* Send boundary deltas */
    auto boundary_deltas = ghost_mgr.collect_boundary_deltas(changed_local_ids, membership);
    uint32_t nd = (uint32_t)boundary_deltas.size();
    send_u32(master_sock, nd);
    if (nd > 0)
        send_all(master_sock, boundary_deltas.data(), nd * sizeof(BoundaryDelta));

    /* Send timing feedback (6 doubles) */
    send_double(master_sock, wall_ms);
    send_double(master_sock, dpu_bfs_ms);
    send_double(master_sock, dpu_clust_ms);
    send_double(master_sock, mis_ms);
    send_double(master_sock, send_ms);
    send_double(master_sock, recv_ms);

    /* NO corrections received */
    /* NO ghost refresh received */
}

/* ===========================================================================
 * MAIN
 * =========================================================================== */

int main(int argc, char *argv[]) {
    if (argc < 3) {
        cerr << "Usage: " << argv[0] << " <listen_port> <dpu_host:dpu_port>" << endl;
        return 1;
    }

    int listen_port = atoi(argv[1]);
    string dpu_addr(argv[2]);

    char hostname[256];
    gethostname(hostname, sizeof(hostname));
    cout << "[Slave " << hostname << "] Starting NOSYNC on port " << listen_port << endl;

    /* ================================================================
     * PHASE 0: WAIT FOR MASTER CONNECTION
     * ================================================================ */
    int server_sock = create_server(listen_port);
    cout << "[Slave " << hostname << "] READY on port " << listen_port << endl;
    cout.flush();

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

    auto t_gi0 = chrono::high_resolution_clock::now();
    vector<uint8_t> membership = sg.membership;
    vector<uint32_t> local_to_global = sg.local_to_global;

    GhostManager ghost_mgr;
    ghost_mgr.init(meta.owned_count, local_to_global, sg.ghost_nodes,
                   sg.ghost_owner, sg.boundary_nodes);
    auto t_gi1 = chrono::high_resolution_clock::now();
    double ghost_init_ms = chrono::duration_cast<chrono::microseconds>(t_gi1 - t_gi0).count() / 1000.0;

    /* ================================================================
     * PHASE 2: BUILD HOST GRAPH + CONNECT TO DPU
     * ================================================================ */
    auto t_hg0 = chrono::high_resolution_clock::now();
    HostGraph host_graph;
    host_graph.build_from_csr(meta.local_nv, sg.csr_offsets, sg.csr_nbrs);
    auto t_hg1 = chrono::high_resolution_clock::now();
    double host_graph_ms = chrono::duration_cast<chrono::microseconds>(t_hg1 - t_hg0).count() / 1000.0;
    cout << "[Slave " << hostname << "] HostGraph built (" << fixed << setprecision(1) << host_graph_ms << " ms)" << endl;

    /* Connect to DPU */
    auto t_cd0 = chrono::high_resolution_clock::now();
    auto [dpu_host, dpu_port] = parse_host_port(dpu_addr);
    int dpu_sock = connect_to_dpu(dpu_host.c_str(), dpu_port);
    if (dpu_sock < 0) {
        cerr << "[Slave " << hostname << "] ERROR: Cannot connect to DPU at " << dpu_addr << endl;
        close(master_sock); close(server_sock);
        return 1;
    }
    auto t_cd1 = chrono::high_resolution_clock::now();
    double connect_dpu_ms = chrono::duration_cast<chrono::microseconds>(t_cd1 - t_cd0).count() / 1000.0;

    auto t_sg0 = chrono::high_resolution_clock::now();
    if (send_graph_to_dpu(dpu_sock, meta.local_nv, sg.csr_offsets, sg.csr_nbrs, membership) < 0) {
        cerr << "[Slave " << hostname << "] ERROR: Failed to send graph to DPU" << endl;
        close(dpu_sock); close(master_sock); close(server_sock);
        return 1;
    }
    auto t_sg1 = chrono::high_resolution_clock::now();
    double send_graph_dpu_ms = chrono::duration_cast<chrono::microseconds>(t_sg1 - t_sg0).count() / 1000.0;
    cout << "[Slave " << hostname << "] Graph sent to DPU at " << dpu_addr
         << " (" << fixed << setprecision(1) << send_graph_dpu_ms << " ms)" << endl;

    /* Free CSR copies */
    sg.csr_offsets.clear(); sg.csr_offsets.shrink_to_fit();
    sg.csr_nbrs.clear(); sg.csr_nbrs.shrink_to_fit();

    /* Send setup timings to master (5 doubles) */
    send_double(master_sock, recv_part_ms);
    send_double(master_sock, ghost_init_ms);
    send_double(master_sock, host_graph_ms);
    send_double(master_sock, connect_dpu_ms);
    send_double(master_sock, send_graph_dpu_ms);

    cout << "[Slave " << hostname << "] Setup: recv_part=" << fixed << setprecision(1) << recv_part_ms
         << "ms ghost_init=" << ghost_init_ms
         << "ms host_graph=" << host_graph_ms
         << "ms connect_dpu=" << connect_dpu_ms
         << "ms send_graph=" << send_graph_dpu_ms << "ms" << endl;

    /* ================================================================
     * PHASE 3: OVERLAPPED PIPELINE (NO SYNC)
     * ================================================================ */
    DpuClusterResponse pending_clust;
    pending_clust.ok = false;
    vector<RawEdge> pending_batch_edges;
    GraphSyncPayload pending_sync;

    int round = 0;
    bool first_batch = true;

    while (true) {
        /* Receive batch edges from master */
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

        if (first_batch) {
            /* ---- PRIMING: first batch, no overlap ---- */
            first_batch = false;

            auto t_send0 = chrono::high_resolution_clock::now();
            if (send_batch_with_sync(dpu_sock, local_edges, pending_sync) < 0) {
                cerr << "[Slave " << hostname << "] Send failed (prime)" << endl; break;
            }
            auto t_send1 = chrono::high_resolution_clock::now();
            pending_clust = recv_cluster_results(dpu_sock);
            auto t_recv1 = chrono::high_resolution_clock::now();

            if (!pending_clust.ok) {
                cerr << "[Slave " << hostname << "] BFS+clustering failed (prime)" << endl; break;
            }

            double prime_send_ms = chrono::duration_cast<chrono::microseconds>(t_send1 - t_send0).count() / 1000.0;
            double prime_recv_ms = chrono::duration_cast<chrono::microseconds>(t_recv1 - t_send1).count() / 1000.0;
            double prime_dpu_bfs_ms = pending_clust.bfs_us / 1000.0;
            double prime_dpu_clust_ms = pending_clust.clust_us / 1000.0;

            auto t_prime_end = chrono::high_resolution_clock::now();
            double prime_wall_ms = chrono::duration_cast<chrono::microseconds>(t_prime_end - t_iter_start).count() / 1000.0;

            pending_batch_edges = local_edges;

            /* Send empty deltas for priming batch */
            tcp_send_deltas_nosync(master_sock, {}, membership, ghost_mgr,
                                    prime_wall_ms, prime_dpu_bfs_ms, prime_dpu_clust_ms,
                                    0, prime_send_ms, prime_recv_ms);

            cout << "[Slave " << hostname << " R" << round << "] (prime) "
                 << "send=" << fixed << setprecision(3) << prime_send_ms
                 << "ms dpu_bfs=" << prime_dpu_bfs_ms
                 << "ms dpu_clust=" << prime_dpu_clust_ms
                 << "ms recv=" << prime_recv_ms
                 << "ms wall=" << prime_wall_ms << "ms" << endl;

            round++;
            continue;
        }

        /* ================================================================
         * STEADY STATE: OVERLAPPED PIPELINE
         * ================================================================ */
        DpuClusterResponse new_clust;
        double dpu_bfs_ms = 0, dpu_clust_ms = 0, send_ms = 0, recv_ms = 0;

        /* Stage A: DPU BFS+clustering (background thread) */
        thread dpu_thread([&]() {
            auto ts0 = chrono::high_resolution_clock::now();
            if (send_batch_with_sync(dpu_sock, local_edges, pending_sync) < 0) {
                new_clust.ok = false; return;
            }
            auto ts1 = chrono::high_resolution_clock::now();
            new_clust = recv_cluster_results(dpu_sock);
            auto ts2 = chrono::high_resolution_clock::now();
            send_ms = chrono::duration_cast<chrono::microseconds>(ts1 - ts0).count() / 1000.0;
            recv_ms = chrono::duration_cast<chrono::microseconds>(ts2 - ts1).count() / 1000.0;
            if (new_clust.ok) {
                dpu_bfs_ms = new_clust.bfs_us / 1000.0;
                dpu_clust_ms = new_clust.clust_us / 1000.0;
            }
        });

        /* Stage B: CPU MIS update for PREVIOUS batch */
        auto t_mis0 = chrono::high_resolution_clock::now();
        OwnedMisResult mis_result = cpu_mis_update_from_clusters(
            pending_clust, pending_batch_edges, host_graph, membership, ghost_mgr);
        auto t_mis1 = chrono::high_resolution_clock::now();
        double mis_ms = chrono::duration_cast<chrono::microseconds>(t_mis1 - t_mis0).count() / 1000.0;

        dpu_thread.join();

        if (!new_clust.ok) {
            cerr << "[Slave " << hostname << "] DPU failed at round " << round << endl; break;
        }

        auto t_iter_end = chrono::high_resolution_clock::now();
        double wall_ms = chrono::duration_cast<chrono::microseconds>(t_iter_end - t_iter_start).count() / 1000.0;

        /* Send deltas + timing — NO sync back */
        tcp_send_deltas_nosync(master_sock, mis_result.changed_local_ids, membership,
                                ghost_mgr, wall_ms, dpu_bfs_ms, dpu_clust_ms,
                                mis_ms, send_ms, recv_ms);

        /* Rotate pipeline state */
        pending_clust = move(new_clust);
        pending_batch_edges = move(local_edges);
        pending_sync = move(mis_result.sync);

        cout << "[Slave " << hostname << " R" << round << "] "
             << "send=" << fixed << setprecision(3) << send_ms
             << "ms dpu_bfs=" << dpu_bfs_ms
             << "ms dpu_clust=" << dpu_clust_ms
             << "ms recv=" << recv_ms
             << "ms MIS=" << mis_ms
             << "ms wall=" << wall_ms << "ms" << endl;

        round++;
    }

    /* ---- DRAIN last pending batch ---- */
    if (pending_clust.ok && !pending_batch_edges.empty()) {
        cpu_mis_update_from_clusters(pending_clust, pending_batch_edges, host_graph, membership, ghost_mgr);
        cout << "[Slave " << hostname << "] Final MIS update done" << endl;
    }

    /* Shutdown DPU */
    if (dpu_sock >= 0) {
        uint32_t shutdown_val = SHUTDOWN_SENTINEL;
        send_all(dpu_sock, &shutdown_val, 4);
        close(dpu_sock);
    }

    /* Send final owned membership to master */
    vector<DeltaEntry> final_mem;
    for (uint32_t i = 0; i < meta.owned_count; i++)
        final_mem.push_back({local_to_global[i], (uint32_t)membership[i]});
    send_vec(master_sock, final_mem);

    close(master_sock);
    close(server_sock);

    cout << "[Slave " << hostname << "] Done. " << round << " rounds." << endl;
    return 0;
}
