/*
 * FullRDS_MIS_dpu.c - DPU Server: BFS (P1) + Clustering (P2) for MIS
 *
 * Based on final_set/CPU_DPU_CLUST/CPU_DPU_CLUST_dpu.c (working reference)
 *
 * Protocol:
 *   Init:
 *     [4B nv] [4B ne] [(nv+1)*4B offsets] [ne*4B nbrs] [nv*1B membership]
 *
 *   Per batch Host->DPU:
 *     [4B num_edges]               (or 0xFFFFFFFF to shutdown)
 *     [num_edges * 8B]             RawEdge (src, dst)
 *     [4B num_edge_mutations]      edge changes from previous MIS update
 *     [num_edge_mut * 12B]         EdgeMutation (src, dst, action)
 *     [4B num_mem_changes]         membership changes from previous MIS update
 *     [num_mem_chg * 8B]           DeltaEntry (node_id, new_val)
 *
 *   Per batch DPU->Host:
 *     [4B num_clusters]
 *     For each cluster: [4B cnt] [cnt * 12B ClusterEdge (src, dst, is_ins)]
 *     [8B bfs_us]
 *     [8B clust_us]
 *
 * Compile: gcc -O2 -o FullRDS_MIS_dpu FullRDS_MIS_dpu.c
 * Run:     ./FullRDS_MIS_dpu [port]
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <stdint.h>
#include <sys/time.h>

static int PORT = 5000;
#define SHUTDOWN_SENTINEL 0xFFFFFFFF
#define HOP_LIMIT 2

/* ---- Network ---- */

static int recv_all(int fd, void *buf, size_t len) {
    size_t r = 0;
    while (r < len) {
        ssize_t n = recv(fd, (char *)buf + r, len - r, 0);
        if (n <= 0) return -1;
        r += n;
    }
    return 0;
}

static int send_all(int fd, const void *buf, size_t len) {
    size_t s = 0;
    while (s < len) {
        ssize_t n = send(fd, (const char *)buf + s, len - s, 0);
        if (n <= 0) return -1;
        s += n;
    }
    return 0;
}

/* ---- Graph (sorted adjacency with contiguous neighbor pool) ---- */

typedef struct { uint32_t *nbrs; uint32_t count, cap; } AdjList;

static AdjList *adj = NULL;
static uint8_t *mem = NULL;
static uint32_t nv = 0;

static uint32_t *nbr_pool = NULL;
static size_t nbr_pool_size = 0;

static int is_pooled(const uint32_t *ptr) {
    return ptr >= nbr_pool && ptr < nbr_pool + nbr_pool_size;
}

/* BFS scratch */
static uint32_t *vis_ver = NULL;
static uint32_t cur_ver = 0;

/* Affected areas */
static uint32_t *aff_buf = NULL;
static size_t aff_buf_cap = 0;
static uint32_t *aff_starts = NULL;
static int *aff_counts = NULL;
static uint32_t aff_batch_cap = 0;

/* Clustering */
static int32_t *node_to_clust = NULL;
typedef struct { int *eidx; int count, cap; } Cluster;
static Cluster *clusters = NULL;
static int n_clusters = 0, clusters_cap = 0;

/* Graph sync structs */
typedef struct { uint32_t src, dst, action; } EdgeMutation;
typedef struct { uint32_t node_id, new_val; } DeltaEntry;

/* ---- Sorted adjacency helpers ---- */

static uint32_t bs_pos(const uint32_t *a, uint32_t n, uint32_t v) {
    uint32_t lo = 0, hi = n;
    while (lo < hi) { uint32_t m = lo + (hi - lo) / 2; if (a[m] < v) lo = m + 1; else hi = m; }
    return lo;
}

static int is_adj(uint32_t u, uint32_t v) {
    if (u >= nv) return 0;
    uint32_t p = bs_pos(adj[u].nbrs, adj[u].count, v);
    return p < adj[u].count && adj[u].nbrs[p] == v;
}

static void adj_ins(uint32_t u, uint32_t v) {
    AdjList *a = &adj[u];
    uint32_t p = bs_pos(a->nbrs, a->count, v);
    if (p < a->count && a->nbrs[p] == v) return;   /* no duplicates */
    if (a->count >= a->cap) {
        uint32_t new_cap = a->cap ? a->cap * 2 : 8;
        uint32_t *new_buf = (uint32_t *)malloc(new_cap * sizeof(uint32_t));
        if (!new_buf) return;
        if (a->nbrs && a->count > 0)
            memcpy(new_buf, a->nbrs, a->count * sizeof(uint32_t));
        if (a->nbrs && !is_pooled(a->nbrs))
            free(a->nbrs);
        a->nbrs = new_buf;
        a->cap = new_cap;
    }
    memmove(&a->nbrs[p + 1], &a->nbrs[p], (a->count - p) * sizeof(uint32_t));
    a->nbrs[p] = v;
    a->count++;
}

static void adj_rem(uint32_t u, uint32_t v) {
    AdjList *a = &adj[u];
    uint32_t p = bs_pos(a->nbrs, a->count, v);
    if (p >= a->count || a->nbrs[p] != v) return;
    memmove(&a->nbrs[p], &a->nbrs[p + 1], (a->count - p - 1) * sizeof(uint32_t));
    a->count--;
}

static void edge_insert(uint32_t u, uint32_t v) { adj_ins(u, v); adj_ins(v, u); }
static void edge_remove(uint32_t u, uint32_t v) { adj_rem(u, v); adj_rem(v, u); }

/* ---- Free graph ---- */

static void free_graph(void) {
    if (adj) {
        for (uint32_t i = 0; i < nv; i++)
            if (adj[i].nbrs && !is_pooled(adj[i].nbrs))
                free(adj[i].nbrs);
        free(adj); adj = NULL;
    }
    free(nbr_pool);      nbr_pool = NULL; nbr_pool_size = 0;
    free(mem);            mem = NULL;
    free(vis_ver);        vis_ver = NULL;
    free(node_to_clust);  node_to_clust = NULL;
    free(aff_buf);        aff_buf = NULL; aff_buf_cap = 0;
    free(aff_starts);     aff_starts = NULL;
    free(aff_counts);     aff_counts = NULL; aff_batch_cap = 0;
}

/* ---- BFS 2-hop affected area ---- */

static int bfs_affected(uint32_t src, uint32_t dst, uint32_t *out, int max_out) {
    cur_ver++;
    int count = 0;
    out[count++] = src; vis_ver[src] = cur_ver;
    if (dst != src && dst < nv) { out[count++] = dst; vis_ver[dst] = cur_ver; }

    int lev_start = 0;
    for (int hop = 0; hop < HOP_LIMIT; hop++) {
        int lev_end = count;
        for (int i = lev_start; i < lev_end; i++) {
            uint32_t nd = out[i];
            for (uint32_t j = 0; j < adj[nd].count; j++) {
                uint32_t nb = adj[nd].nbrs[j];
                if (vis_ver[nb] != cur_ver) {
                    vis_ver[nb] = cur_ver;
                    out[count++] = nb;
                    if (count >= max_out) return count;
                }
            }
        }
        lev_start = lev_end;
    }
    return count;
}

/* ---- Clustering ---- */

static void cluster_add(int ci, int ei) {
    Cluster *c = &clusters[ci];
    if (c->count >= c->cap) {
        c->cap = c->cap ? c->cap * 2 : 8;
        c->eidx = (int *)realloc(c->eidx, c->cap * sizeof(int));
    }
    c->eidx[c->count++] = ei;
}

static int alloc_cluster(void) {
    if (n_clusters >= clusters_cap) {
        int old = clusters_cap;
        clusters_cap = clusters_cap ? clusters_cap * 2 : 64;
        clusters = (Cluster *)realloc(clusters, clusters_cap * sizeof(Cluster));
        for (int i = old; i < clusters_cap; i++) {
            clusters[i].eidx = NULL; clusters[i].count = 0; clusters[i].cap = 0;
        }
    }
    int ci = n_clusters++;
    clusters[ci].count = 0;
    return ci;
}

typedef struct { uint32_t src, dst; int is_ins; } ProcEdge;

static void do_clustering(ProcEdge *edges, int ne) {
    memset(node_to_clust, 0xFF, nv * sizeof(int32_t));

    for (int i = 0; i < n_clusters; i++) {
        free(clusters[i].eidx);
        clusters[i].eidx = NULL; clusters[i].count = 0; clusters[i].cap = 0;
    }
    n_clusters = 0;

    for (int i = 0; i < ne; i++) {
        int overlap[128], n_ov = 0;
        uint32_t *aff = aff_buf + aff_starts[i];
        int ac = aff_counts[i];

        for (int j = 0; j < ac; j++) {
            int32_t c = node_to_clust[aff[j]];
            if (c != -1) {
                int found = 0;
                for (int k = 0; k < n_ov; k++) if (overlap[k] == c) { found = 1; break; }
                if (!found && n_ov < 128) overlap[n_ov++] = c;
            }
        }

        int target;
        if (n_ov == 0) {
            target = alloc_cluster();
        } else {
            target = overlap[0];
            for (int k = 1; k < n_ov; k++)
                if (clusters[overlap[k]].count > clusters[target].count) target = overlap[k];
            for (int k = 0; k < n_ov; k++) {
                int oc = overlap[k];
                if (oc != target) {
                    for (int j = 0; j < clusters[oc].count; j++)
                        cluster_add(target, clusters[oc].eidx[j]);
                    clusters[oc].count = 0;
                    free(clusters[oc].eidx);
                    clusters[oc].eidx = NULL; clusters[oc].cap = 0;
                }
            }
        }

        cluster_add(target, i);
        for (int j = 0; j < ac; j++) node_to_clust[aff[j]] = target;
    }
}

/* ---- Load graph from CSR ---- */

static int load_graph(uint32_t *offsets, uint32_t *sorted_nbrs, uint8_t *init_mem,
                       uint32_t n, uint32_t ne) {
    free_graph();
    nv = n;

    adj = (AdjList *)calloc(nv, sizeof(AdjList));
    mem = (uint8_t *)malloc(nv);
    vis_ver = (uint32_t *)calloc(nv, sizeof(uint32_t));
    node_to_clust = (int32_t *)malloc(nv * sizeof(int32_t));
    cur_ver = 0;

    aff_buf_cap = (nv > 10000000) ? 16000000UL : 4000000UL;
    if (aff_buf_cap > nv) aff_buf_cap = nv;
    aff_buf = (uint32_t *)malloc(aff_buf_cap * sizeof(uint32_t));

    if (!adj || !mem || !vis_ver || !node_to_clust || !aff_buf) {
        fprintf(stderr, "[MIS-P12-DPU] FATAL: alloc failed (%u nodes)\n", nv);
        return -1;
    }

    memcpy(mem, init_mem, nv);

    if (ne > 0) {
        nbr_pool = (uint32_t *)malloc((size_t)ne * sizeof(uint32_t));
        if (!nbr_pool) {
            fprintf(stderr, "[MIS-P12-DPU] FATAL: neighbor pool alloc failed (%.1f GB)\n",
                    (double)ne * 4.0 / (1024.0 * 1024.0 * 1024.0));
            return -1;
        }
        nbr_pool_size = ne;
        memcpy(nbr_pool, sorted_nbrs, (size_t)ne * sizeof(uint32_t));
    }

    for (uint32_t i = 0; i < nv; i++) {
        uint32_t cnt = offsets[i + 1] - offsets[i];
        adj[i].count = cnt;
        adj[i].cap = cnt;
        adj[i].nbrs = (cnt > 0) ? nbr_pool + offsets[i] : NULL;
    }

    printf("[MIS-P12-DPU] Memory: adj=%.0f MB, pool=%.0f MB, vis=%.0f MB, n2c=%.0f MB, aff=%.0f MB\n",
           (double)nv * sizeof(AdjList) / (1024.0 * 1024.0),
           (double)ne * 4.0 / (1024.0 * 1024.0),
           (double)nv * 4.0 / (1024.0 * 1024.0),
           (double)nv * 4.0 / (1024.0 * 1024.0),
           (double)aff_buf_cap * 4.0 / (1024.0 * 1024.0));
    return 0;
}

/* ---- Edge packet ---- */
typedef struct { uint32_t src, dst; } RawEdge;

/* ---- Ensure batch scratch arrays ---- */

static void ensure_batch_capacity(uint32_t num_edges) {
    if (num_edges <= aff_batch_cap) return;
    free(aff_starts);
    free(aff_counts);
    aff_batch_cap = num_edges + 256;
    aff_starts = (uint32_t *)malloc((aff_batch_cap + 1) * sizeof(uint32_t));
    aff_counts = (int *)malloc(aff_batch_cap * sizeof(int));
    if (!aff_starts || !aff_counts) {
        fprintf(stderr, "[MIS-P12-DPU] FATAL: batch scratch alloc failed for %u edges\n", num_edges);
        aff_batch_cap = 0;
    }
}

/* ---- Apply graph sync from host ---- */

static int apply_graph_sync(int client_fd) {
    /* 1. Edge mutations */
    uint32_t num_edge_mut;
    if (recv_all(client_fd, &num_edge_mut, 4) < 0) return -1;

    if (num_edge_mut > 0) {
        EdgeMutation *muts = (EdgeMutation *)malloc(num_edge_mut * sizeof(EdgeMutation));
        if (!muts) return -1;
        if (recv_all(client_fd, muts, num_edge_mut * sizeof(EdgeMutation)) < 0) { free(muts); return -1; }

        for (uint32_t i = 0; i < num_edge_mut; i++) {
            if (muts[i].src >= nv || muts[i].dst >= nv) continue;
            if (muts[i].action == 1)
                edge_insert(muts[i].src, muts[i].dst);
            else
                edge_remove(muts[i].src, muts[i].dst);
        }
        free(muts);
    }

    /* 2. Membership changes */
    uint32_t num_mem_chg;
    if (recv_all(client_fd, &num_mem_chg, 4) < 0) return -1;

    if (num_mem_chg > 0) {
        DeltaEntry *deltas = (DeltaEntry *)malloc(num_mem_chg * sizeof(DeltaEntry));
        if (!deltas) return -1;
        if (recv_all(client_fd, deltas, num_mem_chg * sizeof(DeltaEntry)) < 0) { free(deltas); return -1; }

        for (uint32_t i = 0; i < num_mem_chg; i++) {
            if (deltas[i].node_id < nv)
                mem[deltas[i].node_id] = (uint8_t)deltas[i].new_val;
        }
        free(deltas);
    }

    return 0;
}

/* ---- Send cluster results ---- */

static int send_cluster_results(int client_fd, ProcEdge *proc, uint64_t bfs_us, uint64_t clust_us) {
    uint32_t num_active = 0;
    size_t pack_size = 0;
    for (int c = 0; c < n_clusters; c++) {
        if (clusters[c].count > 0) {
            num_active++;
            pack_size += 4 + (size_t)clusters[c].count * 12;
        }
    }

    char *pack_buf = NULL;
    if (pack_size > 0) {
        pack_buf = (char *)malloc(pack_size);
        if (!pack_buf) return -1;

        size_t off = 0;
        for (int c = 0; c < n_clusters; c++) {
            if (clusters[c].count == 0) continue;
            uint32_t cnt = (uint32_t)clusters[c].count;
            memcpy(pack_buf + off, &cnt, 4); off += 4;

            for (int j = 0; j < clusters[c].count; j++) {
                int ei = clusters[c].eidx[j];
                uint32_t triple[3] = { proc[ei].src, proc[ei].dst, (uint32_t)proc[ei].is_ins };
                memcpy(pack_buf + off, triple, 12); off += 12;
            }
        }
    }

    if (send_all(client_fd, &num_active, 4) < 0) { free(pack_buf); return -1; }
    if (pack_size > 0 && send_all(client_fd, pack_buf, pack_size) < 0) { free(pack_buf); return -1; }
    if (send_all(client_fd, &bfs_us, 8) < 0) { free(pack_buf); return -1; }
    if (send_all(client_fd, &clust_us, 8) < 0) { free(pack_buf); return -1; }

    free(pack_buf);
    return 0;
}

/* ---- Main ---- */

int main(int argc, char *argv[]) {
    if (argc > 1) PORT = atoi(argv[1]);
    int server_fd, client_fd;
    struct sockaddr_in addr;

    server_fd = socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(PORT);
    bind(server_fd, (struct sockaddr *)&addr, sizeof(addr));
    listen(server_fd, 1);

    printf("[MIS-P12-DPU] BFS+Clustering Server on port %d.\n", PORT);
    printf("[MIS-P12-DPU] MIS update offloaded to host CPU. Press Ctrl+C to stop.\n\n");

    while (1) {
        printf("[MIS-P12-DPU] Waiting for host connection...\n");
        client_fd = accept(server_fd, NULL, NULL);
        if (client_fd < 0) continue;
        printf("[MIS-P12-DPU] Host connected.\n");

        /* Receive graph */
        uint32_t n, ne;
        if (recv_all(client_fd, &n, 4) < 0 || recv_all(client_fd, &ne, 4) < 0) goto done;

        printf("[MIS-P12-DPU] Receiving graph: %u vertices, %u CSR edges (%.1f GB)...\n",
               n, ne, ((size_t)(n + 1) * 4 + (size_t)ne * 4 + n) / (1024.0 * 1024.0 * 1024.0));

        uint32_t *off = (uint32_t *)malloc((size_t)(n + 1) * sizeof(uint32_t));
        uint32_t *nbrs = (ne > 0) ? (uint32_t *)malloc((size_t)ne * sizeof(uint32_t)) : NULL;
        uint8_t *init_mem = (uint8_t *)malloc(n);

        if (!off || !init_mem || (ne > 0 && !nbrs)) {
            fprintf(stderr, "[MIS-P12-DPU] FATAL: recv buffer alloc failed\n");
            free(off); free(nbrs); free(init_mem); goto done;
        }

        if (recv_all(client_fd, off, (size_t)(n + 1) * 4) < 0) { free(off); free(nbrs); free(init_mem); goto done; }
        if (ne > 0 && recv_all(client_fd, nbrs, (size_t)ne * 4) < 0) { free(off); free(nbrs); free(init_mem); goto done; }
        if (recv_all(client_fd, init_mem, n) < 0) { free(off); free(nbrs); free(init_mem); goto done; }

        if (load_graph(off, nbrs, init_mem, n, ne) < 0) { free(off); free(nbrs); free(init_mem); goto done; }
        free(off); free(nbrs); free(init_mem);

        printf("[MIS-P12-DPU] Graph loaded. Ready for batches.\n");

        /* Batch loop */
        int batch_num = 0;
        while (1) {
            uint32_t num_edges;
            if (recv_all(client_fd, &num_edges, 4) < 0) {
                printf("[MIS-P12-DPU] Host disconnected.\n"); break;
            }

            if (num_edges == SHUTDOWN_SENTINEL) {
                printf("[MIS-P12-DPU] Session complete.\n"); break;
            }

            batch_num++;

            if (num_edges == 0) {
                if (apply_graph_sync(client_fd) < 0) break;
                uint32_t zero = 0; uint64_t zero64 = 0;
                send_all(client_fd, &zero, 4);
                send_all(client_fd, &zero64, 8);
                send_all(client_fd, &zero64, 8);
                continue;
            }

            ensure_batch_capacity(num_edges);
            if (aff_batch_cap == 0) break;

            /* Step 0: Receive batch edges */
            RawEdge *raw = (RawEdge *)malloc(num_edges * sizeof(RawEdge));
            if (!raw) break;
            if (recv_all(client_fd, raw, num_edges * sizeof(RawEdge)) < 0) { free(raw); break; }

            /* Step 1: Receive and apply graph sync from host */
            if (apply_graph_sync(client_fd) < 0) { free(raw); break; }

            /* Step 2: Adjacency check (classify ins/del) */
            ProcEdge *proc = (ProcEdge *)malloc(num_edges * sizeof(ProcEdge));
            if (!proc) { free(raw); break; }
            for (uint32_t i = 0; i < num_edges; i++) {
                proc[i].src = raw[i].src;
                proc[i].dst = raw[i].dst;
                proc[i].is_ins = !is_adj(proc[i].src, proc[i].dst);
            }
            free(raw);

            /* Step 3: BFS (timed) */
            struct timeval t0, t_bfs, t1;
            gettimeofday(&t0, NULL);

            uint32_t aff_total = 0;
            for (uint32_t i = 0; i < num_edges; i++) {
                aff_starts[i] = aff_total;
                int remain = (int)(aff_buf_cap - aff_total);
                if (remain < 3) {
                    for (uint32_t j = i; j < num_edges; j++) {
                        aff_starts[j] = aff_total;
                        aff_counts[j] = 0;
                    }
                    break;
                }
                int cnt = bfs_affected(proc[i].src, proc[i].dst, aff_buf + aff_total, remain);
                aff_counts[i] = cnt;
                aff_total += cnt;
            }

            gettimeofday(&t_bfs, NULL);
            uint64_t bfs_us = (t_bfs.tv_sec - t0.tv_sec) * 1000000ULL + (t_bfs.tv_usec - t0.tv_usec);

            /* Step 4: Clustering (timed) */
            do_clustering(proc, num_edges);

            gettimeofday(&t1, NULL);
            uint64_t clust_us = (t1.tv_sec - t_bfs.tv_sec) * 1000000ULL + (t1.tv_usec - t_bfs.tv_usec);

            /* Step 5: Send cluster results */
            if (send_cluster_results(client_fd, proc, bfs_us, clust_us) < 0) { free(proc); break; }

            printf("[MIS-P12-DPU] Batch %d: %u edges, bfs=%lu us, clust=%lu us, %d clusters\n",
                   batch_num, num_edges, (unsigned long)bfs_us, (unsigned long)clust_us, n_clusters);
            free(proc);
        }

done:
        close(client_fd);
        printf("[MIS-P12-DPU] Ready for next connection.\n\n");
    }

    close(server_fd);
    return 0;
}
