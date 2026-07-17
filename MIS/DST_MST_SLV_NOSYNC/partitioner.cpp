/*
 * partitioner.cpp - METIS graph partitioning + 2-hop ghost halo extraction
 */

#include "partitioner.h"
#include <metis.h>
#include <iostream>
#include <algorithm>
#include <unordered_set>
#include <unordered_map>
#include <queue>
#include <cstring>

using namespace std;

/* ---- METIS partitioning ---- */

PartitionResult partition_graph(uint32_t nv,
                                 const vector<vector<uint32_t>> &adj,
                                 int K) {
    PartitionResult result;
    result.num_parts = K;

    if (K <= 1) {
        /* Trivial: everything in partition 0 */
        result.part.assign(nv, 0);
        result.edge_cut = 0;
        cout << "[Partitioner] K=1, trivial partition" << endl;
        return result;
    }

    cout << "[Partitioner] METIS partitioning into " << K << " parts..." << endl;

    /* Convert adjacency to METIS CSR format */
    idx_t nvtxs = (idx_t)nv;
    vector<idx_t> xadj(nv + 1);
    xadj[0] = 0;
    for (uint32_t i = 0; i < nv; i++)
        xadj[i + 1] = xadj[i] + (idx_t)adj[i].size();

    vector<idx_t> adjncy(xadj[nv]);
    idx_t idx = 0;
    for (uint32_t i = 0; i < nv; i++)
        for (uint32_t nb : adj[i])
            adjncy[idx++] = (idx_t)nb;

    idx_t ncon = 1;
    idx_t nparts = (idx_t)K;
    idx_t objval;  /* edge cut */
    vector<idx_t> metis_part(nv);

    /* METIS options */
    idx_t options[METIS_NOPTIONS];
    METIS_SetDefaultOptions(options);
    options[METIS_OPTION_OBJTYPE] = METIS_OBJTYPE_CUT;  /* minimize edge cut */
    options[METIS_OPTION_SEED]    = 42;

    int ret = METIS_PartGraphKway(
        &nvtxs, &ncon,
        xadj.data(), adjncy.data(),
        NULL, NULL, NULL,   /* no vertex/edge weights */
        &nparts,
        NULL, NULL,          /* no target partition weights */
        options,
        &objval,
        metis_part.data()
    );

    if (ret != METIS_OK) {
        cerr << "[Partitioner] METIS_PartGraphKway failed with code " << ret << endl;
        /* Fallback: hash partitioning */
        result.part.resize(nv);
        for (uint32_t i = 0; i < nv; i++) result.part[i] = (int32_t)(i % K);
        result.edge_cut = -1;
        return result;
    }

    result.part.resize(nv);
    for (uint32_t i = 0; i < nv; i++)
        result.part[i] = (int32_t)metis_part[i];
    result.edge_cut = (int64_t)objval;

    /* Print partition stats */
    vector<int> pcounts(K, 0);
    for (uint32_t i = 0; i < nv; i++) pcounts[result.part[i]]++;
    cout << "[Partitioner] Edge cut: " << result.edge_cut << endl;
    for (int p = 0; p < K; p++)
        cout << "[Partitioner]   Partition " << p << ": " << pcounts[p] << " nodes" << endl;

    return result;
}

/* ---- Subgraph extraction with 2-hop ghost halo ---- */

SubGraph extract_subgraph(uint32_t nv,
                            const vector<vector<uint32_t>> &adj,
                            const vector<uint8_t> &membership,
                            const vector<int32_t> &part,
                            int p,
                            size_t max_ghost) {
    SubGraph sg;

    /* Step 1: Collect owned nodes */
    vector<uint32_t> owned;
    owned.reserve(nv / part.size());  /* rough estimate */
    for (uint32_t v = 0; v < nv; v++)
        if (part[v] == p) owned.push_back(v);
    sort(owned.begin(), owned.end());

    /* Fast lookup for owned set */
    unordered_set<uint32_t> owned_set(owned.begin(), owned.end());

    /* Step 2: Find boundary nodes (owned with neighbor in other partition) */
    unordered_set<uint32_t> boundary_set;
    for (uint32_t v : owned) {
        for (uint32_t nb : adj[v]) {
            if (part[nb] != p) {
                boundary_set.insert(v);
                break;
            }
        }
    }

    /* Step 3: BFS 2-hop outward from boundary to build ghost halo */
    unordered_set<uint32_t> ghost_set;
    unordered_map<uint32_t, int32_t> ghost_owner_map;

    /* Hop 1: direct neighbors of boundary that are in other partitions */
    vector<uint32_t> hop1;
    for (uint32_t bv : boundary_set) {
        for (uint32_t nb : adj[bv]) {
            if (owned_set.count(nb) == 0 && ghost_set.count(nb) == 0) {
                ghost_set.insert(nb);
                ghost_owner_map[nb] = part[nb];
                hop1.push_back(nb);
                if (ghost_set.size() >= max_ghost) break;
            }
        }
        if (ghost_set.size() >= max_ghost) break;
    }

    /* Hop 2: neighbors of hop1 nodes that aren't owned or already ghost */
    if (ghost_set.size() < max_ghost) {
        for (uint32_t h1 : hop1) {
            for (uint32_t nb : adj[h1]) {
                if (owned_set.count(nb) == 0 && ghost_set.count(nb) == 0) {
                    ghost_set.insert(nb);
                    ghost_owner_map[nb] = part[nb];
                    if (ghost_set.size() >= max_ghost) break;
                }
            }
            if (ghost_set.size() >= max_ghost) break;
        }
    }

    /* Step 4: Assign local IDs */
    /* [0..owned_count): owned nodes, sorted by global ID */
    /* [owned_count..local_nv): ghost nodes, sorted by global ID */
    sg.owned_count = (uint32_t)owned.size();

    vector<uint32_t> ghost_vec(ghost_set.begin(), ghost_set.end());
    sort(ghost_vec.begin(), ghost_vec.end());

    sg.local_nv = sg.owned_count + (uint32_t)ghost_vec.size();
    sg.local_to_global.resize(sg.local_nv);
    for (uint32_t i = 0; i < sg.owned_count; i++)
        sg.local_to_global[i] = owned[i];
    for (uint32_t i = 0; i < (uint32_t)ghost_vec.size(); i++)
        sg.local_to_global[sg.owned_count + i] = ghost_vec[i];

    /* Build reverse map: global -> local */
    unordered_map<uint32_t, uint32_t> global_to_local;
    global_to_local.reserve(sg.local_nv);
    for (uint32_t i = 0; i < sg.local_nv; i++)
        global_to_local[sg.local_to_global[i]] = i;

    /* Step 5: Build CSR in local-ID space */
    /* Include edges between included nodes (owned∪ghost) */
    sg.csr_offsets.resize(sg.local_nv + 1, 0);

    /* First pass: count edges per local node */
    for (uint32_t li = 0; li < sg.local_nv; li++) {
        uint32_t gi = sg.local_to_global[li];
        uint32_t count = 0;
        for (uint32_t nb : adj[gi]) {
            if (global_to_local.count(nb)) count++;
        }
        sg.csr_offsets[li + 1] = count;
    }

    /* Prefix sum */
    for (uint32_t i = 0; i < sg.local_nv; i++)
        sg.csr_offsets[i + 1] += sg.csr_offsets[i];

    sg.local_ne = sg.csr_offsets[sg.local_nv];
    sg.csr_nbrs.resize(sg.local_ne);

    /* Second pass: fill neighbor arrays (in local IDs, sorted) */
    vector<uint32_t> cursor(sg.local_nv, 0);
    for (uint32_t li = 0; li < sg.local_nv; li++) {
        uint32_t gi = sg.local_to_global[li];
        uint32_t off = sg.csr_offsets[li];
        uint32_t cnt = 0;
        for (uint32_t nb : adj[gi]) {
            auto it = global_to_local.find(nb);
            if (it != global_to_local.end()) {
                sg.csr_nbrs[off + cnt] = it->second;
                cnt++;
            }
        }
        /* Sort local neighbor IDs */
        sort(sg.csr_nbrs.begin() + off, sg.csr_nbrs.begin() + off + cnt);
    }

    /* Step 6: Copy membership */
    sg.membership.resize(sg.local_nv);
    for (uint32_t i = 0; i < sg.local_nv; i++)
        sg.membership[i] = membership[sg.local_to_global[i]];

    /* Step 7: Ghost info */
    sg.ghost_nodes = ghost_vec;
    sg.ghost_owner.resize(ghost_vec.size());
    for (size_t i = 0; i < ghost_vec.size(); i++)
        sg.ghost_owner[i] = ghost_owner_map[ghost_vec[i]];

    /* Step 8: Boundary nodes */
    sg.boundary_nodes.assign(boundary_set.begin(), boundary_set.end());
    sort(sg.boundary_nodes.begin(), sg.boundary_nodes.end());

    cout << "[Partitioner] Partition " << p << ": "
         << sg.owned_count << " owned, "
         << ghost_vec.size() << " ghost, "
         << sg.boundary_nodes.size() << " boundary, "
         << sg.local_ne << " CSR edges" << endl;

    return sg;
}
