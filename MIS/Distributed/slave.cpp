/*
 * slave.cpp - Slave node for Distributed CPU-Only MIS Pipeline
 *
 * Runs on cn04/cn05 via SLURM. Listens for master connection.
 * Runs all 3 phases (BFS + Clustering + MIS update) locally on CPU.
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
#include <deque>
#include <cmath>
#include <unistd.h>
#include <omp.h>

#include "protocol.h"
#include "graph_utils.h"
#include "ghost_manager.h"

using namespace std;

/* ===========================================================================
 * PHASE 1: BFS 2-HOP NEIGHBORHOOD (adapted for HostGraph)
 * =========================================================================== */

static vector<uint32_t> bfs_neighborhood(uint32_t vertex, const HostGraph &graph, int hop_limit) {
    vector<uint32_t> nbhood;
    nbhood.reserve(64);

    vector<bool> visited(graph.nv, false);
    deque<pair<uint32_t, int>> q;

    q.push_back({vertex, 0});
    visited[vertex] = true;
    nbhood.push_back(vertex);

    while (!q.empty()) {
        auto [cur, hops] = q.front();
        q.pop_front();
        if (hops >= hop_limit) continue;
        for (uint32_t nb : graph.neighbors(cur)) {
            if (!visited[nb]) {
                visited[nb] = true;
                nbhood.push_back(nb);
                q.push_back({nb, hops + 1});
            }
        }
    }
    return nbhood;
}

static unordered_map<uint32_t, vector<uint32_t>> compute_bfs_neighborhoods(
        const vector<RawEdge> &edges, const HostGraph &graph, int hop_limit, int n_threads) {

    /* Extract unique vertices */
    unordered_set<uint32_t> vset;
    for (auto &e : edges) { vset.insert(e.src); vset.insert(e.dst); }
    vector<uint32_t> vertices(vset.begin(), vset.end());

    unordered_map<uint32_t, vector<uint32_t>> neighborhoods;

    int nn_threads = ((int)vertices.size() / n_threads < 5) ? 1 : n_threads;

    #pragma omp parallel for num_threads(nn_threads) schedule(dynamic)
    for (size_t i = 0; i < vertices.size(); i++) {
        uint32_t v = vertices[i];
        auto nbhood = bfs_neighborhood(v, graph, hop_limit);
        #pragma omp critical
        {
            neighborhoods[v] = move(nbhood);
        }
    }
    return neighborhoods;
}

/* ===========================================================================
 * PHASE 2: VERTEX CLUSTERING
 * =========================================================================== */

struct Cluster {
    vector<uint32_t> vertices;
    vector<uint32_t> edge_indices;  /* indices into batch edges */
};

static vector<Cluster> cluster_vertices(
        const vector<RawEdge> &edges, const HostGraph &graph,
        const unordered_map<uint32_t, vector<uint32_t>> &neighborhoods) {

    /* Extract unique vertices */
    unordered_set<uint32_t> vset;
    for (auto &e : edges) { vset.insert(e.src); vset.insert(e.dst); }
    vector<uint32_t> vertices(vset.begin(), vset.end());

    vector<Cluster> clusters;
    vector<int> nodeToCluster(graph.nv, -1);
    int nextClusterId = 0;

    for (uint32_t vertex : vertices) {
        if (nodeToCluster[vertex] != -1) continue;

        auto it = neighborhoods.find(vertex);
        if (it == neighborhoods.end()) continue;
        const vector<uint32_t> &nbhood = it->second;

        /* Find overlapping clusters */
        unordered_set<int> overlapping;
        for (uint32_t nb : nbhood) {
            if (nb < graph.nv && nodeToCluster[nb] != -1)
                overlapping.insert(nodeToCluster[nb]);
        }

        int finalCid;
        if (overlapping.empty()) {
            finalCid = nextClusterId++;
            clusters.resize(nextClusterId);
        } else if (overlapping.size() == 1) {
            finalCid = *overlapping.begin();
        } else {
            /* Merge into largest */
            finalCid = *overlapping.begin();
            size_t maxSize = clusters[finalCid].vertices.size();
            for (int cid : overlapping) {
                if (cid < (int)clusters.size() && clusters[cid].vertices.size() > maxSize) {
                    maxSize = clusters[cid].vertices.size();
                    finalCid = cid;
                }
            }
            for (int cid : overlapping) {
                if (cid != finalCid && cid < (int)clusters.size()) {
                    for (uint32_t v : clusters[cid].vertices)
                        nodeToCluster[v] = finalCid;
                    clusters[finalCid].vertices.insert(
                        clusters[finalCid].vertices.end(),
                        clusters[cid].vertices.begin(),
                        clusters[cid].vertices.end());
                    clusters[cid].vertices.clear();
                }
            }
        }

        clusters[finalCid].vertices.push_back(vertex);
        for (uint32_t nb : nbhood) {
            if (nb < graph.nv)
                nodeToCluster[nb] = finalCid;
        }
    }

    /* Assign edges to clusters */
    for (uint32_t i = 0; i < (uint32_t)edges.size(); i++) {
        int cs = (edges[i].src < graph.nv) ? nodeToCluster[edges[i].src] : -1;
        int cd = (edges[i].dst < graph.nv) ? nodeToCluster[edges[i].dst] : -1;
        int cid = (cs >= 0) ? cs : cd;
        if (cid >= 0 && cid < (int)clusters.size())
            clusters[cid].edge_indices.push_back(i);
    }

    /* Remove empty clusters */
    clusters.erase(
        remove_if(clusters.begin(), clusters.end(),
                  [](const Cluster& c) { return c.vertices.empty(); }),
        clusters.end());

    return clusters;
}

/* ===========================================================================
 * PHASE 3: OWNERSHIP-AWARE MIS UPDATE
 * =========================================================================== */

static vector<uint32_t> cpu_mis_update(
        const vector<Cluster> &clusters,
        const vector<RawEdge> &batch_edges,
        HostGraph &graph,
        vector<uint8_t> &membership,
        const GhostManager &ghost_mgr) {

    vector<uint32_t> changed;

    for (auto &cluster : clusters) {
        for (uint32_t ei : cluster.edge_indices) {
            uint32_t src = batch_edges[ei].src;
            uint32_t dst = batch_edges[ei].dst;
            if (src >= graph.nv || dst >= graph.nv || src == dst) continue;

            bool is_ins = !graph.is_adjacent(src, dst);

            if (is_ins) {
                /* Edge insertion */
                if (membership[src] && membership[dst]) {
                    uint32_t rem = min(src, dst);
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
            }
        }
    }

    sort(changed.begin(), changed.end());
    changed.erase(unique(changed.begin(), changed.end()), changed.end());
    return changed;
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

    auto t_gi0 = chrono::high_resolution_clock::now();
    vector<uint8_t> membership = sg.membership;
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
    int hop_limit = 2;

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

        /* Phase 1: BFS Neighborhoods (CPU) */
        auto t_bfs0 = chrono::high_resolution_clock::now();
        auto neighborhoods = compute_bfs_neighborhoods(local_edges, host_graph, hop_limit, n_threads);
        auto t_bfs1 = chrono::high_resolution_clock::now();
        double bfs_ms = chrono::duration_cast<chrono::microseconds>(t_bfs1 - t_bfs0).count() / 1000.0;

        /* Phase 2: Clustering (CPU) */
        auto t_clust0 = chrono::high_resolution_clock::now();
        auto clusters = cluster_vertices(local_edges, host_graph, neighborhoods);
        auto t_clust1 = chrono::high_resolution_clock::now();
        double clust_ms = chrono::duration_cast<chrono::microseconds>(t_clust1 - t_clust0).count() / 1000.0;

        /* Phase 3: MIS Update + Topology (CPU) */
        auto t_mis0 = chrono::high_resolution_clock::now();
        auto changed = cpu_mis_update(clusters, local_edges, host_graph, membership, ghost_mgr);
        auto t_mis1 = chrono::high_resolution_clock::now();
        double mis_ms = chrono::duration_cast<chrono::microseconds>(t_mis1 - t_mis0).count() / 1000.0;

        auto t_iter_end = chrono::high_resolution_clock::now();
        double wall_ms = chrono::duration_cast<chrono::microseconds>(t_iter_end - t_iter_start).count() / 1000.0;

        /* Send boundary deltas to master */
        auto boundary_deltas = ghost_mgr.collect_boundary_deltas(changed, membership);
        uint32_t nd = (uint32_t)boundary_deltas.size();
        send_u32(master_sock, nd);
        if (nd > 0)
            send_all(master_sock, boundary_deltas.data(), nd * sizeof(BoundaryDelta));

        /* Send timing feedback (4 doubles) */
        send_double(master_sock, wall_ms);
        send_double(master_sock, bfs_ms);
        send_double(master_sock, clust_ms);
        send_double(master_sock, mis_ms);

        /* Receive corrections */
        uint32_t nc = recv_u32(master_sock);
        if (nc > 0) {
            vector<BoundaryDelta> corrections(nc);
            recv_all(master_sock, corrections.data(), nc * sizeof(BoundaryDelta));
            for (auto &c : corrections) {
                uint32_t lid = ghost_mgr.to_local(c.global_node_id);
                if (lid != UINT32_MAX && lid < (uint32_t)membership.size())
                    membership[lid] = c.new_membership;
            }
        }

        /* Ghost refresh */
        uint32_t do_refresh = recv_u32(master_sock);
        if (do_refresh) {
            uint32_t nr = recv_u32(master_sock);
            if (nr > 0) {
                vector<DeltaEntry> refresh(nr);
                recv_all(master_sock, refresh.data(), nr * sizeof(DeltaEntry));
                for (auto &d : refresh) {
                    uint32_t lid = ghost_mgr.to_local(d.node_id);
                    if (lid != UINT32_MAX && ghost_mgr.is_ghost(lid) && lid < (uint32_t)membership.size())
                        membership[lid] = (uint8_t)d.new_val;
                }
            }
        }

        cout << "[Slave " << hostname << " R" << round << "] "
             << "bfs=" << fixed << setprecision(3) << bfs_ms
             << "ms clust=" << clust_ms
             << "ms MIS=" << mis_ms
             << "ms wall=" << wall_ms << "ms"
             << " clusters=" << clusters.size()
             << " deltas=" << nd << endl;

        round++;
    }

    /* Send final owned membership to master */
    vector<DeltaEntry> final_mem;
    for (uint32_t i = 0; i < meta.owned_count; i++)
        final_mem.push_back({local_to_global[i], (uint32_t)membership[i]});
    send_vec(master_sock, final_mem);

    close(master_sock);

    cout << "[Slave " << hostname << "] Done. " << round << " rounds. Waiting for next connection..." << endl;
    cout.flush();

    } /* end while(true) — loop back to accept next master connection */

    close(server_sock);
    return 0;
}
