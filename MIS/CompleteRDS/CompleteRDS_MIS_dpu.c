/*
 * CompleteRDS_MIS_dpu.c - DPU Server: Full Offload (BFS + Clustering + MIS Update)
 *
 * DPU handles ALL three phases:
 *   P1: 2-hop BFS affected area
 *   P2: Clustering (union-find on overlapping areas)
 *   P3: MIS Update (insertion/deletion lemmas) + Graph Mutation
 *
 * No sync back-channel needed - DPU owns the graph entirely.
 *
 * Protocol:
 *   Init:  [4B nv] [4B ne] [(nv+1)*4B offsets] [ne*4B nbrs] [nv*1B membership]
 *   Batch Host->DPU: [4B num_edges] [8B*E RawEdge(src,dst)]
 *   Batch DPU->Host: [4B num_edge_mutations] [12B*M EdgeMutation(src,dst,action)]
 *                     [4B num_mem_changes] [8B*C DeltaEntry(node_id,new_val)]
 *                     [8B bfs_us] [8B clust_us] [8B mis_us]
 *   End:   [4B 0xFFFFFFFF]
 *
 * Based on: final_set/CPU_DPU_CLUST_MIS/CPU_DPU_CLUST_MIS_dpu.c
 *
 * Compile: gcc -O2 -o CompleteRDS_MIS_dpu CompleteRDS_MIS_dpu.c
 * Run:     ./CompleteRDS_MIS_dpu [port]
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <stdint.h>
#include <sys/time.h>

#define DEFAULT_PORT 5004
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

/* ---- Graph (sorted adjacency lists with contiguous neighbor pool) ---- */

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

/* MIS update tracking */
typedef struct { uint32_t src, dst, action; } EdgeMutation;
typedef struct { uint32_t node_id, new_val; } DeltaEntry;

static EdgeMutation *edge_muts = NULL;
static uint32_t edge_muts_count = 0, edge_muts_cap = 0;

static DeltaEntry *mem_changes = NULL;
static uint32_t mem_changes_count = 0, mem_changes_cap = 0;

static uint32_t *changed_nodes = NULL;
static uint32_t changed_count = 0, changed_cap = 0;

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
    if (p < a->count && a->nbrs[p] == v) return; /* already present */
    if (a->count >= a->cap) {
        uint32_t new_cap = a->cap ? a->cap * 2 : 8;
        uint32_t *new_buf = (uint32_t *)malloc(new_cap * sizeof(uint32_t));
        if (!new_buf) { fprintf(stderr, "[DPU P123 MIS] adj_ins malloc failed\n"); return; }
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

/* ---- Dynamic array helpers ---- */

static void add_edge_mut(uint32_t src, uint32_t dst, uint32_t action) {
    if (edge_muts_count >= edge_muts_cap) {
        edge_muts_cap = edge_muts_cap ? edge_muts_cap * 2 : 1024;
        edge_muts = (EdgeMutation *)realloc(edge_muts, edge_muts_cap * sizeof(EdgeMutation));
    }
    edge_muts[edge_muts_count].src = src;
    edge_muts[edge_muts_count].dst = dst;
    edge_muts[edge_muts_count].action = action;
    edge_muts_count++;
}

static void add_changed(uint32_t node) {
    if (changed_count >= changed_cap) {
        changed_cap = changed_cap ? changed_cap * 2 : 1024;
        changed_nodes = (uint32_t *)realloc(changed_nodes, changed_cap * sizeof(uint32_t));
    }
    changed_nodes[changed_count++] = node;
}

/* ---- Free graph memory ---- */

static void free_graph(void) {
    if (adj) {
        for (uint32_t i = 0; i < nv; i++)
            if (adj[i].nbrs && !is_pooled(adj[i].nbrs))
                free(adj[i].nbrs);
        free(adj);
        adj = NULL;
    }
    free(nbr_pool);   nbr_pool = NULL; nbr_pool_size = 0;
    free(mem);         mem = NULL;
    free(vis_ver);     vis_ver = NULL;
    free(node_to_clust); node_to_clust = NULL;
    free(aff_buf);     aff_buf = NULL; aff_buf_cap = 0;
    free(aff_starts);  aff_starts = NULL;
    free(aff_counts);  aff_counts = NULL; aff_batch_cap = 0;
}

/* ---- BFS affected area ---- */

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

/* ---- MIS Update (Phase 3 on DPU) ---- */

static void handle_insertion(uint32_t u, uint32_t v) {
    if (u >= nv || v >= nv) return;
    if (mem[u] && mem[v]) {
        /* Remove lower-ID vertex from MIS */
        uint32_t rem = (u < v) ? u : v;
        mem[rem] = 0;
        add_changed(rem);
        /* Try to add rem's neighbors that now have no MIS neighbor */
        for (uint32_t j = 0; j < adj[rem].count; j++) {
            uint32_t nb = adj[rem].nbrs[j];
            if (!mem[nb]) {
                int ok = 1;
                for (uint32_t k = 0; k < adj[nb].count; k++) {
                    if (mem[adj[nb].nbrs[k]]) { ok = 0; break; }
                }
                if (ok) { mem[nb] = 1; add_changed(nb); }
            }
        }
    }
    /* Insert edge into graph */
    edge_insert(u, v);
    add_edge_mut(u, v, 1);
}

static void handle_deletion(uint32_t u, uint32_t v) {
    if (u >= nv || v >= nv) return;
    /* Remove edge first so adjacency check is correct */
    edge_remove(u, v);
    add_edge_mut(u, v, 0);

    if (mem[u] && !mem[v]) {
        /* v lost MIS neighbor u; check if v can join MIS */
        int ok = 1;
        for (uint32_t k = 0; k < adj[v].count; k++) {
            if (mem[adj[v].nbrs[k]]) { ok = 0; break; }
        }
        if (ok) { mem[v] = 1; add_changed(v); }
    } else if (!mem[u] && mem[v]) {
        int ok = 1;
        for (uint32_t k = 0; k < adj[u].count; k++) {
            if (mem[adj[u].nbrs[k]]) { ok = 0; break; }
        }
        if (ok) { mem[u] = 1; add_changed(u); }
    }
}

static void do_mis_update(ProcEdge *proc, int ne) {
    edge_muts_count = 0;
    changed_count = 0;

    for (int c = 0; c < n_clusters; c++) {
        if (clusters[c].count == 0) continue;
        for (int j = 0; j < clusters[c].count; j++) {
            int ei = clusters[c].eidx[j];
            if (proc[ei].is_ins)
                handle_insertion(proc[ei].src, proc[ei].dst);
            else
                handle_deletion(proc[ei].src, proc[ei].dst);
        }
    }

    /* Build deduplicated membership changes (insertion sort + unique) */
    for (uint32_t i = 1; i < changed_count; i++) {
        uint32_t key = changed_nodes[i];
        int j = (int)i - 1;
        while (j >= 0 && changed_nodes[j] > key) {
            changed_nodes[j + 1] = changed_nodes[j];
            j--;
        }
        changed_nodes[j + 1] = key;
    }
    uint32_t unique_count = 0;
    for (uint32_t i = 0; i < changed_count; i++) {
        if (i == 0 || changed_nodes[i] != changed_nodes[i - 1])
            changed_nodes[unique_count++] = changed_nodes[i];
    }

    if (unique_count > mem_changes_cap) {
        mem_changes_cap = unique_count + 256;
        mem_changes = (DeltaEntry *)realloc(mem_changes, mem_changes_cap * sizeof(DeltaEntry));
    }
    mem_changes_count = unique_count;
    for (uint32_t i = 0; i < unique_count; i++) {
        mem_changes[i].node_id = changed_nodes[i];
        mem_changes[i].new_val = mem[changed_nodes[i]];
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
        fprintf(stderr, "[DPU P123 MIS] FATAL: Failed to allocate graph metadata (%u nodes)\n", nv);
        return -1;
    }

    memcpy(mem, init_mem, nv);

    if (ne > 0) {
        nbr_pool = (uint32_t *)malloc((size_t)ne * sizeof(uint32_t));
        if (!nbr_pool) {
            fprintf(stderr, "[DPU P123 MIS] FATAL: Failed to allocate neighbor pool (%u entries)\n", ne);
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

    printf("[DPU P123 MIS] Memory: adj=%.0f MB, pool=%.0f MB, vis=%.0f MB\n",
           (double)nv * sizeof(AdjList) / (1024.0 * 1024.0),
           (double)ne * 4.0 / (1024.0 * 1024.0),
           (double)nv * 4.0 / (1024.0 * 1024.0));

    return 0;
}

typedef struct { uint32_t src, dst; } RawEdge;

static void ensure_batch_capacity(uint32_t num_edges) {
    if (num_edges <= aff_batch_cap) return;
    free(aff_starts);
    free(aff_counts);
    aff_batch_cap = num_edges + 256;
    aff_starts = (uint32_t *)malloc((aff_batch_cap + 1) * sizeof(uint32_t));
    aff_counts = (int *)malloc(aff_batch_cap * sizeof(int));
    if (!aff_starts || !aff_counts) {
        fprintf(stderr, "[DPU P123 MIS] FATAL: Failed to allocate batch scratch for %u edges\n", num_edges);
        aff_batch_cap = 0;
    }
}

/* ---- Send results: edge mutations + membership changes + timings ---- */

static int send_results(int client_fd, uint64_t bfs_us, uint64_t clust_us, uint64_t mis_us) {
    if (send_all(client_fd, &edge_muts_count, 4) < 0) return -1;
    if (edge_muts_count > 0) {
        if (send_all(client_fd, edge_muts, edge_muts_count * sizeof(EdgeMutation)) < 0) return -1;
    }

    if (send_all(client_fd, &mem_changes_count, 4) < 0) return -1;
    if (mem_changes_count > 0) {
        if (send_all(client_fd, mem_changes, mem_changes_count * sizeof(DeltaEntry)) < 0) return -1;
    }

    if (send_all(client_fd, &bfs_us, 8) < 0) return -1;
    if (send_all(client_fd, &clust_us, 8) < 0) return -1;
    if (send_all(client_fd, &mis_us, 8) < 0) return -1;

    return 0;
}

/* ---- Main ---- */

int main(int argc, char *argv[]) {
    int port = DEFAULT_PORT;
    if (argc > 1) port = atoi(argv[1]);
    if (port <= 0 || port > 65535) port = DEFAULT_PORT;

    int server_fd, client_fd;
    struct sockaddr_in addr;

    server_fd = socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port);
    bind(server_fd, (struct sockaddr *)&addr, sizeof(addr));
    listen(server_fd, 1);

    printf("[DPU P123 MIS] Full Offload Server on port %d.\n", port);
    printf("[DPU P123 MIS] DPU handles: BFS + Clustering + MIS Update + Graph Mutation.\n");
    printf("[DPU P123 MIS] CPU handles: batch preparation only.\n\n");

    while (1) {
        printf("[DPU P123 MIS] Waiting for host connection...\n");
        client_fd = accept(server_fd, NULL, NULL);
        if (client_fd < 0) continue;
        printf("[DPU P123 MIS] Host connected.\n");

        /* ---- Receive graph ---- */
        uint32_t n, ne;
        if (recv_all(client_fd, &n, 4) < 0 || recv_all(client_fd, &ne, 4) < 0) goto done;

        printf("[DPU P123 MIS] Receiving graph: %u vertices, %u CSR edges...\n", n, ne);

        uint32_t *off = (uint32_t *)malloc((size_t)(n + 1) * sizeof(uint32_t));
        uint32_t *nbrs = (ne > 0) ? (uint32_t *)malloc((size_t)ne * sizeof(uint32_t)) : NULL;
        uint8_t *init_mem = (uint8_t *)malloc(n);

        if (!off || !init_mem || (ne > 0 && !nbrs)) {
            fprintf(stderr, "[DPU P123 MIS] FATAL: malloc failed for graph receive\n");
            free(off); free(nbrs); free(init_mem);
            goto done;
        }

        if (recv_all(client_fd, off, (size_t)(n + 1) * 4) < 0) { free(off); free(nbrs); free(init_mem); goto done; }
        if (ne > 0 && recv_all(client_fd, nbrs, (size_t)ne * 4) < 0) { free(off); free(nbrs); free(init_mem); goto done; }
        if (recv_all(client_fd, init_mem, n) < 0) { free(off); free(nbrs); free(init_mem); goto done; }

        if (load_graph(off, nbrs, init_mem, n, ne) < 0) {
            free(off); free(nbrs); free(init_mem);
            goto done;
        }

        free(off); free(nbrs); free(init_mem);
        printf("[DPU P123 MIS] Graph loaded. Ready for batches.\n");

        /* ---- Batch loop ---- */
        int batch_num = 0;
        while (1) {
            uint32_t num_edges;
            if (recv_all(client_fd, &num_edges, 4) < 0) {
                printf("[DPU P123 MIS] Host disconnected.\n");
                break;
            }

            if (num_edges == SHUTDOWN_SENTINEL) {
                printf("[DPU P123 MIS] Session complete.\n");
                break;
            }

            batch_num++;

            if (num_edges == 0) {
                uint32_t zero = 0;
                uint64_t zero64 = 0;
                send_all(client_fd, &zero, 4);   /* 0 edge mutations */
                send_all(client_fd, &zero, 4);   /* 0 mem changes */
                send_all(client_fd, &zero64, 8); /* bfs_us */
                send_all(client_fd, &zero64, 8); /* clust_us */
                send_all(client_fd, &zero64, 8); /* mis_us */
                continue;
            }

            ensure_batch_capacity(num_edges);
            if (aff_batch_cap == 0) break;

            /* Step 0: Receive batch edges (no sync needed - DPU owns graph) */
            RawEdge *raw = (RawEdge *)malloc(num_edges * sizeof(RawEdge));
            if (!raw) { fprintf(stderr, "[DPU P123 MIS] malloc failed for batch\n"); break; }
            if (recv_all(client_fd, raw, num_edges * sizeof(RawEdge)) < 0) { free(raw); break; }

            /* Step 1: Adjacency check (classify insertion vs deletion) */
            ProcEdge *proc = (ProcEdge *)malloc(num_edges * sizeof(ProcEdge));
            if (!proc) { free(raw); break; }
            for (uint32_t i = 0; i < num_edges; i++) {
                proc[i].src = raw[i].src;
                proc[i].dst = raw[i].dst;
                proc[i].is_ins = !is_adj(proc[i].src, proc[i].dst);
            }
            free(raw);

            /* Step 2: BFS (Phase 1) */
            struct timeval t0, t_bfs, t_clust, t_mis;
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

            /* Step 3: Clustering (Phase 2) */
            do_clustering(proc, num_edges);

            gettimeofday(&t_clust, NULL);
            uint64_t clust_us = (t_clust.tv_sec - t_bfs.tv_sec) * 1000000ULL + (t_clust.tv_usec - t_bfs.tv_usec);

            /* Step 4: MIS Update + Graph Mutation (Phase 3) */
            do_mis_update(proc, num_edges);

            gettimeofday(&t_mis, NULL);
            uint64_t mis_us = (t_mis.tv_sec - t_clust.tv_sec) * 1000000ULL + (t_mis.tv_usec - t_clust.tv_usec);

            /* Step 5: Send results back to host */
            if (send_results(client_fd, bfs_us, clust_us, mis_us) < 0) { free(proc); break; }

            printf("[DPU P123 MIS] Batch %d: %u edges, bfs=%lu us, clust=%lu us, mis=%lu us, %d clusters, "
                   "%u edge_muts, %u mem_chg\n",
                   batch_num, num_edges, (unsigned long)bfs_us, (unsigned long)clust_us, (unsigned long)mis_us,
                   n_clusters, edge_muts_count, mem_changes_count);
            free(proc);
        }

done:
        close(client_fd);
        printf("[DPU P123 MIS] Ready for next connection.\n\n");
    }

    close(server_fd);
    return 0;
}
