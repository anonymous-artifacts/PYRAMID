/*
 * slave.cpp - Slave node for DST_MST_SLV Graph Coloring Pipeline
 *
 * Runs on cn04/cn05 via SLURM. Listens for master connection.
 * Offloads ProcessCE (P1) + CheckConflict (P2) to DPU (ARM Cortex-A72).
 * Runs UpdateNeighbors (P3) on CPU.
 * Uses overlapped pipeline: DPU processes current batch while CPU processes previous.
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
#include <omp.h>

#include "protocol.h"
#include "graph_utils.h"
#include "ghost_manager.h"

using namespace std;

#define MAX_COLORS 1024

/* ---- DPU protocol types ---- */

struct BatchEdge { uint32_t src, dst, is_ins; };
struct ColorChange { uint32_t node_id; int32_t old_color, new_color; };
struct ColorSync { uint32_t node_id; int32_t new_color; };
struct EdgeMutSync { uint32_t src, dst, action; };

/* ---- DPU P1+P2 results ---- */

struct DpuGcResult {
    bool ok;
    vector<uint32_t> affected;         /* vertices needing P3 */
    vector<ColorChange> color_changes; /* all color changes from P1+P2 */
    uint64_t p1_us, p2_us;
};

/* ---- DPU communication functions ---- */

static int send_gc_graph_to_dpu(int sock, uint32_t nv,
                                  const vector<uint32_t> &offsets,
                                  const vector<uint32_t> &nbrs,
                                  const vector<int32_t> &colors) {
    try {
        uint32_t ne = (uint32_t)nbrs.size();
        send_all(sock, &nv, 4);
        send_all(sock, &ne, 4);
        send_all(sock, offsets.data(), (size_t)(nv + 1) * 4);
        if (ne > 0) send_all(sock, nbrs.data(), (size_t)ne * 4);
        send_all(sock, colors.data(), (size_t)nv * 4);
        return 0;
    } catch (...) { return -1; }
}

/* Sync payload from previous P3 results */
struct GcSyncPayload {
    vector<ColorSync> color_syncs;
    vector<EdgeMutSync> edge_syncs;
};

static int send_gc_batch_to_dpu(int sock,
                                  const vector<BatchEdge> &edges,
                                  const GcSyncPayload &sync) {
    try {
        uint32_t ne = (uint32_t)edges.size();
        send_all(sock, &ne, 4);
        if (ne > 0) send_all(sock, edges.data(), ne * sizeof(BatchEdge));

        uint32_t ncs = (uint32_t)sync.color_syncs.size();
        send_all(sock, &ncs, 4);
        if (ncs > 0) send_all(sock, sync.color_syncs.data(), ncs * sizeof(ColorSync));

        uint32_t nes = (uint32_t)sync.edge_syncs.size();
        send_all(sock, &nes, 4);
        if (nes > 0) send_all(sock, sync.edge_syncs.data(), nes * sizeof(EdgeMutSync));

        return 0;
    } catch (...) { return -1; }
}

static DpuGcResult recv_gc_p12_results(int sock) {
    DpuGcResult r;
    r.ok = false;

    try {
        uint32_t num_aff;
        recv_all(sock, &num_aff, 4);
        if (num_aff > 0) {
            r.affected.resize(num_aff);
            recv_all(sock, r.affected.data(), num_aff * 4);
        }

        uint32_t num_cc;
        recv_all(sock, &num_cc, 4);
        if (num_cc > 0) {
            r.color_changes.resize(num_cc);
            recv_all(sock, r.color_changes.data(), num_cc * sizeof(ColorChange));
        }

        uint32_t num_em;
        recv_all(sock, &num_em, 4);

        recv_all(sock, &r.p1_us, 8);
        recv_all(sock, &r.p2_us, 8);

        r.ok = true;
    } catch (...) {}
    return r;
}

/* ---- UpdateNeighbors (Phase 3) on CPU ---- */

struct P3Result {
    vector<uint32_t> changed;
    GcSyncPayload sync;  /* color changes to sync back to DPU */
};

static P3Result update_neighbors_cpu(
        const DpuGcResult &dpu_result,
        const vector<BatchEdge> &batch_edges,
        HostGraph &graph,
        vector<int32_t> &colors,
        vector<int32_t> &prev_colors,
        const GhostManager &ghost_mgr) {

    P3Result result;

    /* Apply color changes from DPU P1+P2 */
    for (auto &cc : dpu_result.color_changes) {
        if (cc.node_id < (uint32_t)colors.size()) {
            colors[cc.node_id] = cc.new_color;
            prev_colors[cc.node_id] = cc.old_color;
        }
    }

    /* Apply topology update (GC: graph update before coloring, DPU already did its own) */
    for (auto &e : batch_edges) {
        if (e.src >= graph.nv || e.dst >= graph.nv) continue;
        if (e.is_ins) {
            graph.insert_edge(e.src, e.dst);
            result.sync.edge_syncs.push_back({e.src, e.dst, 1});
        } else {
            graph.remove_edge(e.src, e.dst);
            result.sync.edge_syncs.push_back({e.src, e.dst, 0});
        }
    }

    /* UpdateNeighbors (P3): propagate freed colors */
    vector<uint32_t> affected = dpu_result.affected;
    int itr = 0;

    while (!affected.empty() && itr < 50) {
        vector<uint32_t> next_aff;
        for (uint32_t v : affected) {
            if (v >= (uint32_t)colors.size()) continue;
            int32_t freed_color = prev_colors[v];
            if (freed_color < 0) continue;

            for (uint32_t u : graph.neighbors(v)) {
                if (u >= (uint32_t)colors.size()) continue;
                if (!ghost_mgr.is_owned(u)) continue;

                if (freed_color < colors[u]) {
                    bool in_SC = false;
                    for (uint32_t nbr : graph.neighbors(u)) {
                        if (nbr < (uint32_t)colors.size() && colors[nbr] == freed_color) {
                            in_SC = true;
                            break;
                        }
                    }
                    if (!in_SC) {
                        int32_t old = colors[u];
                        prev_colors[u] = old;
                        colors[u] = freed_color;
                        next_aff.push_back(u);
                        result.changed.push_back(u);
                        result.sync.color_syncs.push_back({u, freed_color});
                    }
                }
            }
        }
        sort(next_aff.begin(), next_aff.end());
        next_aff.erase(unique(next_aff.begin(), next_aff.end()), next_aff.end());
        affected = next_aff;
        itr++;
    }

    /* Also include P1+P2 changed nodes */
    for (auto &cc : dpu_result.color_changes)
        result.changed.push_back(cc.node_id);

    sort(result.changed.begin(), result.changed.end());
    result.changed.erase(unique(result.changed.begin(), result.changed.end()), result.changed.end());

    return result;
}

/* ---- Translate global → local edge IDs ---- */

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

/* ---- Classify edges as insertion/deletion ---- */

static vector<BatchEdge> classify_edges(const vector<RawEdge> &local_edges,
                                          const HostGraph &graph) {
    vector<BatchEdge> classified;
    classified.reserve(local_edges.size());
    for (auto &e : local_edges) {
        uint32_t is_ins = graph.is_adjacent(e.src, e.dst) ? 0 : 1;
        classified.push_back({e.src, e.dst, is_ins});
    }
    /* Sort: deletions first, then insertions */
    sort(classified.begin(), classified.end(),
         [](const BatchEdge &a, const BatchEdge &b) {
             if (a.is_ins != b.is_ins) return a.is_ins < b.is_ins;
             return a.src < b.src;
         });
    return classified;
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
    cout << "[Slave " << hostname << "] Starting on port " << listen_port << endl;

    /* ================================================================
     * PHASE 0: WAIT FOR MASTER CONNECTION
     * ================================================================ */
    int server_sock = create_server(listen_port);
    cout << "[Slave " << hostname << "] READY on port " << listen_port << endl;
    cout.flush();

    int master_sock = accept_one(server_sock);
    cout << "[Slave " << hostname << "] Master connected" << endl;

    /* ================================================================
     * PHASE 1: RECEIVE PARTITION + COLORS FROM MASTER
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

    /* Receive local colors */
    vector<int32_t> colors(meta.local_nv, -1);
    recv_all(master_sock, colors.data(), meta.local_nv * sizeof(int32_t));
    vector<int32_t> prev_colors(colors);

    auto t_gi0 = chrono::high_resolution_clock::now();
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

    /* Send graph + colors to DPU */
    auto t_sg0 = chrono::high_resolution_clock::now();
    if (send_gc_graph_to_dpu(dpu_sock, meta.local_nv, sg.csr_offsets, sg.csr_nbrs, colors) < 0) {
        cerr << "[Slave " << hostname << "] ERROR: Failed to send graph to DPU" << endl;
        close(dpu_sock); close(master_sock); close(server_sock);
        return 1;
    }
    auto t_sg1 = chrono::high_resolution_clock::now();
    double send_graph_dpu_ms = chrono::duration_cast<chrono::microseconds>(t_sg1 - t_sg0).count() / 1000.0;

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
     * PHASE 3: OVERLAPPED PIPELINE
     * ================================================================ */
    DpuGcResult pending_dpu_result;
    pending_dpu_result.ok = false;
    vector<BatchEdge> pending_batch_edges;
    GcSyncPayload pending_sync;

    int round = 0;
    bool first_batch = true;

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
        vector<BatchEdge> classified = classify_edges(local_edges, host_graph);

        auto t_iter_start = chrono::high_resolution_clock::now();

        if (first_batch) {
            /* ---- PRIMING: first batch, no overlap ---- */
            first_batch = false;

            auto t_send0 = chrono::high_resolution_clock::now();
            if (send_gc_batch_to_dpu(dpu_sock, classified, pending_sync) < 0) {
                cerr << "[Slave " << hostname << "] Send failed (prime)" << endl; break;
            }
            auto t_send1 = chrono::high_resolution_clock::now();
            pending_dpu_result = recv_gc_p12_results(dpu_sock);
            auto t_recv1 = chrono::high_resolution_clock::now();

            if (!pending_dpu_result.ok) {
                cerr << "[Slave " << hostname << "] DPU P1+P2 failed (prime)" << endl; break;
            }

            double prime_send_ms = chrono::duration_cast<chrono::microseconds>(t_send1 - t_send0).count() / 1000.0;
            double prime_recv_ms = chrono::duration_cast<chrono::microseconds>(t_recv1 - t_send1).count() / 1000.0;
            double prime_dpu_p1_ms = pending_dpu_result.p1_us / 1000.0;
            double prime_dpu_p2_ms = pending_dpu_result.p2_us / 1000.0;

            auto t_prime_end = chrono::high_resolution_clock::now();
            double prime_wall_ms = chrono::duration_cast<chrono::microseconds>(t_prime_end - t_iter_start).count() / 1000.0;

            pending_batch_edges = classified;

            /* Empty deltas for first batch (no P3 results yet) */
            uint32_t zero = 0;
            send_u32(master_sock, zero);

            /* Send timing (6 doubles: wall, p1, p2, p3, send, recv) */
            send_double(master_sock, prime_wall_ms);
            send_double(master_sock, prime_dpu_p1_ms);
            send_double(master_sock, prime_dpu_p2_ms);
            send_double(master_sock, 0);  /* no P3 yet */
            send_double(master_sock, prime_send_ms);
            send_double(master_sock, prime_recv_ms);

            /* Receive corrections (none expected for priming) */
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

            cout << "[Slave " << hostname << " R" << round << "] (prime) "
                 << "send=" << fixed << setprecision(3) << prime_send_ms
                 << "ms p1=" << prime_dpu_p1_ms
                 << "ms p2=" << prime_dpu_p2_ms
                 << "ms recv=" << prime_recv_ms
                 << "ms wall=" << prime_wall_ms << "ms" << endl;

            round++;
            continue;
        }

        /* ================================================================
         * STEADY STATE: OVERLAPPED PIPELINE
         * ================================================================ */
        DpuGcResult new_dpu_result;
        double send_ms = 0, recv_ms = 0, dpu_p1_ms = 0, dpu_p2_ms = 0;

        /* Stage A: DPU P1+P2 for current batch (background thread) */
        thread dpu_thread([&]() {
            auto ts0 = chrono::high_resolution_clock::now();
            if (send_gc_batch_to_dpu(dpu_sock, classified, pending_sync) < 0) {
                new_dpu_result.ok = false; return;
            }
            auto ts1 = chrono::high_resolution_clock::now();
            new_dpu_result = recv_gc_p12_results(dpu_sock);
            auto ts2 = chrono::high_resolution_clock::now();
            send_ms = chrono::duration_cast<chrono::microseconds>(ts1 - ts0).count() / 1000.0;
            recv_ms = chrono::duration_cast<chrono::microseconds>(ts2 - ts1).count() / 1000.0;
            if (new_dpu_result.ok) {
                dpu_p1_ms = new_dpu_result.p1_us / 1000.0;
                dpu_p2_ms = new_dpu_result.p2_us / 1000.0;
            }
        });

        /* Stage B: CPU P3 (UpdateNeighbors) for PREVIOUS batch */
        auto t_p3_0 = chrono::high_resolution_clock::now();
        P3Result p3_result = update_neighbors_cpu(
            pending_dpu_result, pending_batch_edges, host_graph,
            colors, prev_colors, ghost_mgr);
        auto t_p3_1 = chrono::high_resolution_clock::now();
        double p3_ms = chrono::duration_cast<chrono::microseconds>(t_p3_1 - t_p3_0).count() / 1000.0;

        dpu_thread.join();

        if (!new_dpu_result.ok) {
            cerr << "[Slave " << hostname << "] DPU failed at round " << round << endl; break;
        }

        auto t_iter_end = chrono::high_resolution_clock::now();
        double wall_ms = chrono::duration_cast<chrono::microseconds>(t_iter_end - t_iter_start).count() / 1000.0;

        /* Send boundary color deltas to master */
        vector<uint32_t> owned_changed;
        for (uint32_t lid : p3_result.changed) {
            if (ghost_mgr.is_owned(lid))
                owned_changed.push_back(lid);
        }
        auto boundary_deltas = ghost_mgr.collect_boundary_color_deltas(owned_changed, colors);
        uint32_t nd = (uint32_t)boundary_deltas.size();
        send_u32(master_sock, nd);
        if (nd > 0)
            send_all(master_sock, boundary_deltas.data(), nd * sizeof(DeltaEntry));

        /* Send timing (6 doubles) */
        send_double(master_sock, wall_ms);
        send_double(master_sock, dpu_p1_ms);
        send_double(master_sock, dpu_p2_ms);
        send_double(master_sock, p3_ms);
        send_double(master_sock, send_ms);
        send_double(master_sock, recv_ms);

        /* Receive corrections */
        uint32_t nc = recv_u32(master_sock);
        if (nc > 0) {
            vector<DeltaEntry> corrections(nc);
            recv_all(master_sock, corrections.data(), nc * sizeof(DeltaEntry));
            ghost_mgr.apply_color_corrections(corrections, colors);
            /* Sync corrections to DPU */
            for (auto &c : corrections)
                p3_result.sync.color_syncs.push_back({c.node_id, (int32_t)c.new_val});
        }

        /* Ghost refresh */
        uint32_t do_refresh = recv_u32(master_sock);
        if (do_refresh) {
            uint32_t nr = recv_u32(master_sock);
            if (nr > 0) {
                vector<DeltaEntry> refresh(nr);
                recv_all(master_sock, refresh.data(), nr * sizeof(DeltaEntry));
                ghost_mgr.apply_ghost_color_refresh(refresh, colors);
                /* Sync ghost refreshes to DPU */
                for (auto &d : refresh) {
                    uint32_t lid = ghost_mgr.to_local(d.node_id);
                    if (lid != UINT32_MAX)
                        p3_result.sync.color_syncs.push_back({lid, (int32_t)d.new_val});
                }
            }
        }

        /* Update prev_colors */
        for (uint32_t i = 0; i < meta.local_nv; i++)
            prev_colors[i] = colors[i];

        /* Rotate pipeline state */
        pending_dpu_result = move(new_dpu_result);
        pending_batch_edges = move(classified);
        pending_sync = move(p3_result.sync);

        cout << "[Slave " << hostname << " R" << round << "] "
             << "send=" << fixed << setprecision(3) << send_ms
             << "ms p1=" << dpu_p1_ms
             << "ms p2=" << dpu_p2_ms
             << "ms recv=" << recv_ms
             << "ms P3=" << p3_ms
             << "ms wall=" << wall_ms << "ms"
             << " deltas=" << nd << endl;

        round++;
    }

    /* ---- DRAIN last pending batch ---- */
    if (pending_dpu_result.ok && !pending_batch_edges.empty()) {
        update_neighbors_cpu(pending_dpu_result, pending_batch_edges, host_graph,
                             colors, prev_colors, ghost_mgr);
        cout << "[Slave " << hostname << "] Final P3 update done" << endl;
    }

    /* Shutdown DPU */
    if (dpu_sock >= 0) {
        uint32_t shutdown_val = SHUTDOWN_SENTINEL;
        send_all(dpu_sock, &shutdown_val, 4);
        close(dpu_sock);
    }

    /* Send final owned colors to master */
    vector<DeltaEntry> final_colors;
    for (uint32_t i = 0; i < meta.owned_count; i++)
        final_colors.push_back({local_to_global[i], (uint32_t)colors[i]});
    send_vec(master_sock, final_colors);

    close(master_sock);
    close(server_sock);

    cout << "[Slave " << hostname << "] Done. " << round << " rounds." << endl;
    return 0;
}
