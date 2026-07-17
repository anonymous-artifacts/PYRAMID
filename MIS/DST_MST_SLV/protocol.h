/*
 * protocol.h - TCP-based protocol for distributed dynamic MIS pipeline v5
 *
 * Replaces MPI with plain TCP sockets. Defines message structs,
 * send_all/recv_all helpers, and connection utilities.
 */

#pragma once

#include <cstdint>
#include <cstring>
#include <vector>
#include <string>
#include <stdexcept>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <errno.h>

/* ---- Core data types (same as v4) ---- */

struct RawEdge {
    uint32_t src, dst;
};

struct DeltaEntry {
    uint32_t node_id, new_val;
};

struct EdgeMutation {
    uint32_t src, dst, action;  /* action: 1=insert, 0=delete */
};

struct BoundaryDelta {
    uint32_t global_node_id;
    uint8_t  new_membership;
    uint8_t  pad[3];
};

struct PartitionMeta {
    uint32_t partition_id;
    uint32_t num_partitions;
    uint32_t local_nv;
    uint32_t local_ne;
    uint32_t owned_count;
    uint32_t num_ghost;
    uint32_t num_boundary;
    uint32_t global_nv;
};

struct TimingFeedback {
    double wall_ms;
    double dpu_bfs_ms;
    double cpu_clust_mis_ms;
    int32_t avg_cluster_size;
    int32_t pad;
};

/* DPU Cluster response (v4: DPU does BFS + clustering) */
struct DpuClusterResponse {
    std::vector<uint32_t> is_ins;
    std::vector<uint32_t> cluster_ids;
    uint32_t num_clusters;
    uint64_t bfs_us;
    uint64_t clust_us;
    bool ok;
};

struct GraphSyncPayload {
    std::vector<EdgeMutation> edge_mutations;
    std::vector<DeltaEntry> membership_changes;
};

struct SubGraph {
    uint32_t local_nv;
    uint32_t local_ne;
    uint32_t owned_count;
    std::vector<uint32_t> csr_offsets;
    std::vector<uint32_t> csr_nbrs;
    std::vector<uint8_t>  membership;
    std::vector<uint32_t> local_to_global;
    std::vector<uint32_t> ghost_nodes;
    std::vector<int32_t>  ghost_owner;
    std::vector<uint32_t> boundary_nodes;
};

#define SHUTDOWN_SENTINEL 0xFFFFFFFF
#define GHOST_REFRESH_INTERVAL 5

/* ---- TCP helpers ---- */

static inline void send_all(int sock, const void *buf, size_t len) {
    const char *p = (const char *)buf;
    size_t sent = 0;
    while (sent < len) {
        ssize_t n = send(sock, p + sent, len - sent, MSG_NOSIGNAL);
        if (n <= 0) {
            if (n < 0 && errno == EINTR) continue;
            throw std::runtime_error("send_all: connection lost");
        }
        sent += n;
    }
}

static inline void recv_all(int sock, void *buf, size_t len) {
    char *p = (char *)buf;
    size_t recvd = 0;
    while (recvd < len) {
        ssize_t n = recv(sock, p + recvd, len - recvd, 0);
        if (n <= 0) {
            if (n < 0 && errno == EINTR) continue;
            throw std::runtime_error("recv_all: connection lost");
        }
        recvd += n;
    }
}

static inline void send_u32(int sock, uint32_t val) {
    send_all(sock, &val, 4);
}

static inline uint32_t recv_u32(int sock) {
    uint32_t val;
    recv_all(sock, &val, 4);
    return val;
}

static inline void send_u64(int sock, uint64_t val) {
    send_all(sock, &val, 8);
}

static inline uint64_t recv_u64(int sock) {
    uint64_t val;
    recv_all(sock, &val, 8);
    return val;
}

static inline void send_double(int sock, double val) {
    send_all(sock, &val, sizeof(double));
}

static inline double recv_double(int sock) {
    double val;
    recv_all(sock, &val, sizeof(double));
    return val;
}

/* Send a vector of trivially-copyable elements */
template<typename T>
static inline void send_vec(int sock, const std::vector<T> &v) {
    uint32_t count = (uint32_t)v.size();
    send_u32(sock, count);
    if (count > 0) {
        send_all(sock, v.data(), count * sizeof(T));
    }
}

/* Receive a vector of trivially-copyable elements */
template<typename T>
static inline std::vector<T> recv_vec(int sock) {
    uint32_t count = recv_u32(sock);
    std::vector<T> v(count);
    if (count > 0) {
        recv_all(sock, v.data(), count * sizeof(T));
    }
    return v;
}

/* ---- Connection utilities ---- */

/* Create a TCP server socket listening on the given port */
static inline int create_server(int port) {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) throw std::runtime_error("socket() failed");

    int opt = 1;
    setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port);

    if (bind(sock, (struct sockaddr *)&addr, sizeof(addr)) < 0)
        throw std::runtime_error("bind() failed on port " + std::to_string(port));
    if (listen(sock, 5) < 0)
        throw std::runtime_error("listen() failed");

    return sock;
}

/* Accept one connection from the server socket */
static inline int accept_one(int server_sock) {
    struct sockaddr_in client_addr{};
    socklen_t len = sizeof(client_addr);
    int conn = accept(server_sock, (struct sockaddr *)&client_addr, &len);
    if (conn < 0) throw std::runtime_error("accept() failed");

    /* Set TCP_NODELAY and large buffers */
    int opt = 1;
    setsockopt(conn, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt));
    int bufsize = 4 * 1024 * 1024;
    setsockopt(conn, SOL_SOCKET, SO_SNDBUF, &bufsize, sizeof(bufsize));
    setsockopt(conn, SOL_SOCKET, SO_RCVBUF, &bufsize, sizeof(bufsize));

    return conn;
}

/* Connect to a remote host:port */
static inline int tcp_connect(const std::string &host, int port) {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) throw std::runtime_error("socket() failed");

    struct hostent *he = gethostbyname(host.c_str());
    if (!he) throw std::runtime_error("gethostbyname failed for: " + host);

    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    memcpy(&addr.sin_addr, he->h_addr_list[0], he->h_length);

    if (connect(sock, (struct sockaddr *)&addr, sizeof(addr)) < 0)
        throw std::runtime_error("connect() failed to " + host + ":" + std::to_string(port));

    int opt = 1;
    setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt));
    int bufsize = 4 * 1024 * 1024;
    setsockopt(sock, SOL_SOCKET, SO_SNDBUF, &bufsize, sizeof(bufsize));
    setsockopt(sock, SOL_SOCKET, SO_RCVBUF, &bufsize, sizeof(bufsize));

    return sock;
}

/* Parse "host:port" string */
static inline std::pair<std::string, int> parse_host_port(const std::string &s) {
    auto colon = s.rfind(':');
    if (colon == std::string::npos)
        throw std::runtime_error("Invalid host:port format: " + s);
    return {s.substr(0, colon), std::stoi(s.substr(colon + 1))};
}

/* ---- Partition distribution helpers (master -> slave) ---- */

static inline void send_partition(int sock, const SubGraph &sg, uint32_t part_id,
                                  uint32_t num_parts, uint32_t global_nv) {
    PartitionMeta meta{};
    meta.partition_id = part_id;
    meta.num_partitions = num_parts;
    meta.local_nv = sg.local_nv;
    meta.local_ne = sg.local_ne;
    meta.owned_count = sg.owned_count;
    meta.num_ghost = (uint32_t)sg.ghost_nodes.size();
    meta.num_boundary = (uint32_t)sg.boundary_nodes.size();
    meta.global_nv = global_nv;
    send_all(sock, &meta, sizeof(meta));

    /* CSR */
    send_all(sock, sg.csr_offsets.data(), (sg.local_nv + 1) * sizeof(uint32_t));
    send_all(sock, sg.csr_nbrs.data(), sg.local_ne * sizeof(uint32_t));

    /* Membership */
    send_all(sock, sg.membership.data(), sg.local_nv);

    /* Local-to-global */
    send_all(sock, sg.local_to_global.data(), sg.local_nv * sizeof(uint32_t));

    /* Ghost info */
    send_all(sock, sg.ghost_nodes.data(), meta.num_ghost * sizeof(uint32_t));
    send_all(sock, sg.ghost_owner.data(), meta.num_ghost * sizeof(int32_t));

    /* Boundary nodes */
    send_all(sock, sg.boundary_nodes.data(), meta.num_boundary * sizeof(uint32_t));
}

static inline SubGraph recv_partition(int sock, PartitionMeta &meta) {
    recv_all(sock, &meta, sizeof(meta));

    SubGraph sg;
    sg.local_nv = meta.local_nv;
    sg.local_ne = meta.local_ne;
    sg.owned_count = meta.owned_count;

    /* CSR */
    sg.csr_offsets.resize(meta.local_nv + 1);
    recv_all(sock, sg.csr_offsets.data(), (meta.local_nv + 1) * sizeof(uint32_t));
    sg.csr_nbrs.resize(meta.local_ne);
    recv_all(sock, sg.csr_nbrs.data(), meta.local_ne * sizeof(uint32_t));

    /* Membership */
    sg.membership.resize(meta.local_nv);
    recv_all(sock, sg.membership.data(), meta.local_nv);

    /* Local-to-global */
    sg.local_to_global.resize(meta.local_nv);
    recv_all(sock, sg.local_to_global.data(), meta.local_nv * sizeof(uint32_t));

    /* Ghost info */
    sg.ghost_nodes.resize(meta.num_ghost);
    recv_all(sock, sg.ghost_nodes.data(), meta.num_ghost * sizeof(uint32_t));
    sg.ghost_owner.resize(meta.num_ghost);
    recv_all(sock, sg.ghost_owner.data(), meta.num_ghost * sizeof(int32_t));

    /* Boundary nodes */
    sg.boundary_nodes.resize(meta.num_boundary);
    recv_all(sock, sg.boundary_nodes.data(), meta.num_boundary * sizeof(uint32_t));

    return sg;
}
