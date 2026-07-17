/*
 * graph_utils.h - Graph I/O, adjacency, MIS computation, network helpers
 *
 * Extracted from T15_host.cpp for reuse in distributed pipeline.
 * All functions are header-only (static/inline).
 *
 * v4 changes:
 *   - Added recv_cluster_results() for new DPU protocol (BFS + clustering)
 */

#pragma once

#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <algorithm>
#include <filesystem>
#include <chrono>
#include <iomanip>
#include <cstring>
#include <cmath>
#include <random>
#include <utility>
#include <numeric>
#include <thread>
#include <mutex>
#include <atomic>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <unistd.h>
#include <stdint.h>
#include <errno.h>
#include <omp.h>

#include "protocol.h"

/* ---- NVTX ---- */
#ifdef USE_NVTX
#include <nvtx3/nvToolsExt.h>
#define NVTX_PUSH(name)   nvtxRangePushA(name)
#define NVTX_POP()         nvtxRangePop()
#define NVTX_MARK(name)    nvtxMarkA(name)
#else
#define NVTX_PUSH(name)    ((void)0)
#define NVTX_POP()          ((void)0)
#define NVTX_MARK(name)     ((void)0)
#endif

namespace fs = std::filesystem;
using namespace std;

/* ===========================================================================
 * HOST GRAPH - Dynamic sorted adjacency for CPU processing
 * =========================================================================== */

struct HostGraph {
    uint32_t nv;
    vector<vector<uint32_t>> adj;

    void build_from_csr(uint32_t n, const vector<uint32_t> &offsets,
                         const vector<uint32_t> &nbrs) {
        nv = n;
        adj.resize(nv);
        for (uint32_t u = 0; u < nv; u++)
            adj[u].assign(nbrs.begin() + offsets[u], nbrs.begin() + offsets[u + 1]);
    }

    bool is_adjacent(uint32_t u, uint32_t v) const {
        if (u >= nv) return false;
        return binary_search(adj[u].begin(), adj[u].end(), v);
    }

    void insert_edge(uint32_t u, uint32_t v) {
        auto ins = [](vector<uint32_t> &a, uint32_t val) {
            auto it = lower_bound(a.begin(), a.end(), val);
            if (it == a.end() || *it != val) a.insert(it, val);
        };
        if (u < nv) ins(adj[u], v);
        if (v < nv) ins(adj[v], u);
    }

    void remove_edge(uint32_t u, uint32_t v) {
        auto rem = [](vector<uint32_t> &a, uint32_t val) {
            auto it = lower_bound(a.begin(), a.end(), val);
            if (it != a.end() && *it == val) a.erase(it);
        };
        if (u < nv) rem(adj[u], v);
        if (v < nv) rem(adj[v], u);
    }

    const vector<uint32_t>& neighbors(uint32_t u) const { return adj[u]; }

    uint32_t degree(uint32_t u) const { return (u < nv) ? (uint32_t)adj[u].size() : 0; }
};

/* ===========================================================================
 * NETWORK HELPERS
 * Now provided by protocol.h (send_all, recv_all throw on error).
 * Wrapper functions below return int for backward compat with DPU protocol code.
 * =========================================================================== */

static int net_send(int fd, const void *buf, size_t len) {
    try { send_all(fd, buf, len); return 0; }
    catch (...) { return -1; }
}

static int net_recv(int fd, void *buf, size_t len) {
    try { recv_all(fd, buf, len); return 0; }
    catch (...) { return -1; }
}

static int connect_to_dpu(const char *host, int port) {
    try {
        int sock = tcp_connect(std::string(host), port);
        cout << "[Net] Connected to DPU at " << host << ":" << port << endl;
        return sock;
    } catch (std::exception &e) {
        cerr << "[Net] DPU connection failed: " << e.what() << endl;
        return -1;
    }
}

/* ===========================================================================
 * DPU PROTOCOL: Send/Receive
 * =========================================================================== */

static int send_graph_to_dpu(int sock, uint32_t nv, const vector<uint32_t> &offsets,
                               const vector<uint32_t> &nbrs, const vector<uint8_t> &membership) {
    uint32_t ne = (uint32_t)nbrs.size();
    if (net_send(sock, &nv, 4) < 0) return -1;
    if (net_send(sock, &ne, 4) < 0) return -1;
    if (net_send(sock, offsets.data(), (nv + 1) * 4) < 0) return -1;
    if (ne > 0 && net_send(sock, nbrs.data(), (size_t)ne * 4) < 0) return -1;
    if (net_send(sock, membership.data(), nv) < 0) return -1;
    return 0;
}

static int send_batch_with_sync(int sock, const vector<RawEdge> &batch,
                                 const GraphSyncPayload &sync) {
    uint32_t ne = batch.size();
    if (net_send(sock, &ne, 4) < 0) return -1;
    if (ne > 0 && net_send(sock, batch.data(), ne * sizeof(RawEdge)) < 0) return -1;

    uint32_t num_mut = sync.edge_mutations.size();
    if (net_send(sock, &num_mut, 4) < 0) return -1;
    if (num_mut > 0 && net_send(sock, sync.edge_mutations.data(), num_mut * sizeof(EdgeMutation)) < 0) return -1;

    uint32_t num_mem = sync.membership_changes.size();
    if (net_send(sock, &num_mem, 4) < 0) return -1;
    if (num_mem > 0 && net_send(sock, sync.membership_changes.data(), num_mem * sizeof(DeltaEntry)) < 0) return -1;

    return 0;
}

/* ===========================================================================
 * v4 DPU CLUSTER RESPONSE: Receive BFS + clustering results
 * =========================================================================== */

static DpuClusterResponse recv_cluster_results(int sock) {
    DpuClusterResponse resp;
    resp.ok = true;
    resp.num_clusters = 0;
    resp.bfs_us = 0;
    resp.clust_us = 0;

    uint32_t num_edges;
    if (net_recv(sock, &num_edges, 4) < 0) { resp.ok = false; return resp; }

    if (num_edges == 0) {
        if (net_recv(sock, &resp.bfs_us, 8) < 0) { resp.ok = false; return resp; }
        if (net_recv(sock, &resp.clust_us, 8) < 0) { resp.ok = false; return resp; }
        return resp;
    }

    resp.is_ins.resize(num_edges);
    resp.cluster_ids.resize(num_edges);
    if (net_recv(sock, resp.is_ins.data(), num_edges * 4) < 0) { resp.ok = false; return resp; }
    if (net_recv(sock, resp.cluster_ids.data(), num_edges * 4) < 0) { resp.ok = false; return resp; }
    if (net_recv(sock, &resp.num_clusters, 4) < 0) { resp.ok = false; return resp; }
    if (net_recv(sock, &resp.bfs_us, 8) < 0) { resp.ok = false; return resp; }
    if (net_recv(sock, &resp.clust_us, 8) < 0) { resp.ok = false; return resp; }

    return resp;
}

/* (v3 legacy CPU clustering functions removed — v5 uses DPU clustering directly) */

/* ===========================================================================
 * PREPROCESSING FUNCTIONS (from T15 Steps A-H)
 * =========================================================================== */

struct EdgeListData { uint32_t num_nodes; vector<pair<uint32_t, uint32_t>> edges; };

static EdgeListData read_raw_edges(const string &filename) {
    EdgeListData data;
    ifstream file(filename);
    if (!file.is_open()) { cerr << "Error: Cannot open " << filename << endl; exit(1); }

    string line;
    uint32_t max_node = 0;
    bool first_data_line = true;

    while (getline(file, line)) {
        if (line.empty() || line[0] == '#' || line[0] == '%') continue;
        istringstream iss(line);
        if (first_data_line) {
            first_data_line = false;
            uint32_t a, b, c;
            if ((iss >> a >> b >> c) && iss.eof()) { max_node = a - 1; continue; }
            iss.clear(); iss.str(line);
        }
        uint32_t u, v;
        if (iss >> u >> v) {
            data.edges.push_back({u, v});
            if (u > max_node) max_node = u;
            if (v > max_node) max_node = v;
        }
    }
    data.num_nodes = max_node + 1;
    cout << "[Preprocess] Read " << data.edges.size() << " edges, " << data.num_nodes << " nodes" << endl;
    return data;
}


static void add_backedges(vector<pair<uint32_t, uint32_t>> &graph) {
    size_t orig = graph.size();
    graph.reserve(orig * 2);
    for (size_t i = 0; i < orig; i++) graph.push_back({graph[i].second, graph[i].first});
    cout << "[Preprocess] Backedges: " << orig << " -> " << graph.size() << endl;
}

static void build_sorted_adj(uint32_t num_nodes,
                               const vector<pair<uint32_t, uint32_t>> &edges,
                               vector<vector<uint32_t>> &adj) {
    adj.resize(num_nodes);
    for (auto &[u, v] : edges) if (u < num_nodes) adj[u].push_back(v);
    #pragma omp parallel for schedule(dynamic, 4096)
    for (uint32_t i = 0; i < num_nodes; i++) {
        sort(adj[i].begin(), adj[i].end());
        adj[i].erase(unique(adj[i].begin(), adj[i].end()), adj[i].end());
    }
    cout << "[Preprocess] Adjacency built for " << num_nodes << " nodes" << endl;
}

static int compute_greedy_mis(uint32_t num_nodes,
                               const vector<vector<uint32_t>> &adj,
                               vector<uint8_t> &membership, int num_threads) {
    cout << "[Preprocess] Computing greedy MIS (" << num_threads << " threads)..." << endl;
    auto t0 = chrono::high_resolution_clock::now();

    membership.assign(num_nodes, 0);
    vector<uint32_t> priority(num_nodes);
    iota(priority.begin(), priority.end(), 0);
    mt19937 rng(12345);
    shuffle(priority.begin(), priority.end(), rng);

    vector<uint32_t> rank_of(num_nodes);
    for (uint32_t i = 0; i < num_nodes; i++) rank_of[priority[i]] = i;

    omp_set_num_threads(num_threads);

    #pragma omp parallel for schedule(dynamic, 1024)
    for (uint32_t i = 0; i < num_nodes; i++) {
        uint32_t v = priority[i];
        bool can_add = true;
        for (uint32_t nb : adj[v])
            if (nb != v && rank_of[nb] < rank_of[v] && membership[nb]) { can_add = false; break; }
        if (can_add) membership[v] = 1;
    }

    int conflicts = 0;
    for (uint32_t v = 0; v < num_nodes; v++) {
        if (!membership[v]) continue;
        for (uint32_t nb : adj[v]) {
            if (nb != v && nb > v && membership[nb]) {
                if (rank_of[v] > rank_of[nb]) membership[v] = 0;
                else membership[nb] = 0;
                conflicts++;
            }
        }
    }
    if (conflicts > 0) cout << "[Preprocess] Fixed " << conflicts << " MIS conflicts" << endl;

    for (uint32_t v = 0; v < num_nodes; v++) {
        if (membership[v]) continue;
        bool can_add = true;
        for (uint32_t nb : adj[v]) if (nb != v && membership[nb]) { can_add = false; break; }
        if (can_add) membership[v] = 1;
    }

    int count = 0;
    for (uint32_t i = 0; i < num_nodes; i++) if (membership[i]) count++;

    auto t1 = chrono::high_resolution_clock::now();
    double ms = chrono::duration_cast<chrono::microseconds>(t1 - t0).count() / 1000.0;
    cout << "[Preprocess] MIS cardinality=" << count << " in " << fixed << setprecision(2) << ms << " ms" << endl;
    return count;
}

static void build_csr(uint32_t num_nodes, const vector<vector<uint32_t>> &adj,
                       vector<uint32_t> &offsets, vector<uint32_t> &nbrs) {
    offsets.resize(num_nodes + 1, 0);
    for (uint32_t i = 0; i < num_nodes; i++) offsets[i + 1] = offsets[i] + adj[i].size();
    nbrs.resize(offsets[num_nodes]);
    uint32_t idx = 0;
    for (uint32_t i = 0; i < num_nodes; i++)
        for (uint32_t nb : adj[i]) nbrs[idx++] = nb;
    cout << "[Preprocess] CSR: " << num_nodes << " nodes, " << nbrs.size() << " entries" << endl;
}

/* ===========================================================================
 * MIS VERIFIER
 * =========================================================================== */

static bool verify_mis(const vector<vector<uint32_t>> &adj, const vector<uint8_t> &mem, uint32_t nv) {
    /* Check independence (skip self-loops) */
    for (uint32_t u = 0; u < nv; u++) {
        if (!mem[u]) continue;
        for (uint32_t v : adj[u]) {
            if (v != u && v < nv && mem[v]) {
                cerr << "[Verify] INDEPENDENCE VIOLATION: both " << u << " and " << v << " in MIS" << endl;
                return false;
            }
        }
    }
    /* Check maximality (skip self-loops) */
    for (uint32_t u = 0; u < nv; u++) {
        if (mem[u]) continue;
        bool has_mis_nb = false;
        for (uint32_t v : adj[u]) {
            if (v != u && v < nv && mem[v]) { has_mis_nb = true; break; }
        }
        if (!has_mis_nb) {
            cerr << "[Verify] MAXIMALITY VIOLATION: node " << u << " has no MIS neighbor" << endl;
            return false;
        }
    }
    return true;
}

static string extract_dataset_name(const string &filepath) {
    return fs::path(filepath).stem().string();
}
