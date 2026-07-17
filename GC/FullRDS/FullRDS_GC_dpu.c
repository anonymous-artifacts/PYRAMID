/*
 * FullRDS_GC_dpu.c - DPU Server: ProcessCE (P1) + CheckConflict (P2) for GC
 *
 * Based on PartialRDS_GC_dpu.c + CheckConflict added
 * Reference: final_set_gc_v2/CPU_DPU_P12_GC/ for protocol
 *
 * Protocol:
 *   Init:
 *     [4B nv] [4B ne] [(nv+1)*4B offsets] [ne*4B nbrs] [nv*4B colors(int32)]
 *
 *   Per batch Host->DPU:
 *     [4B num_edges]               (or 0xFFFFFFFF to shutdown)
 *     [num_edges * 12B]            BatchEdge (src, dst, is_ins)
 *     [4B num_color_syncs]         color changes from previous P3
 *     [num_cs * 8B]                ColorSync (node_id, new_color)
 *     [4B num_edge_syncs]          edge mutations from previous batch
 *     [num_es * 12B]               EdgeSync (src, dst, action)
 *
 *   Per batch DPU->Host:
 *     [4B num_affected]            affected vertices for P3 (UpdateNeighbors)
 *     [num_aff * 4B]               vertex IDs
 *     [4B num_color_changes]       all color changes from P1+P2
 *     [num_cc * 12B]               ColorChange (node_id, old_color, new_color)
 *     [4B num_edge_muts]           always 0 (host does topology update)
 *     [8B p1_us]                   ProcessCE time
 *     [8B p2_us]                   CheckConflict time
 *
 * Compile: gcc -O2 -o FullRDS_GC_dpu FullRDS_GC_dpu.c
 * Run:     ./FullRDS_GC_dpu [port]
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <stdint.h>
#include <sys/time.h>

static int PORT = 5001;
#define SHUTDOWN_SENTINEL 0xFFFFFFFF
#define MAX_COLORS 1024

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
static int32_t *colors = NULL;
static int32_t *prev_colors = NULL;
static uint32_t nv = 0;

static uint32_t *nbr_pool = NULL;
static size_t nbr_pool_size = 0;

static int is_pooled(const uint32_t *ptr) {
    return ptr >= nbr_pool && ptr < nbr_pool + nbr_pool_size;
}

/* ---- Sorted adjacency helpers ---- */

static uint32_t bs_pos(const uint32_t *a, uint32_t n, uint32_t v) {
    uint32_t lo = 0, hi = n;
    while (lo < hi) { uint32_t m = lo + (hi - lo) / 2; if (a[m] < v) lo = m + 1; else hi = m; }
    return lo;
}

static void adj_ins(uint32_t u, uint32_t v) {
    AdjList *a = &adj[u];
    uint32_t p = bs_pos(a->nbrs, a->count, v);
    if (p < a->count && a->nbrs[p] == v) return;
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
    free(nbr_pool);    nbr_pool = NULL; nbr_pool_size = 0;
    free(colors);      colors = NULL;
    free(prev_colors); prev_colors = NULL;
}

/* ---- Smallest available color ---- */

static int32_t smallest_available_color(uint32_t u) {
    uint8_t used[MAX_COLORS];
    memset(used, 0, MAX_COLORS);
    for (uint32_t j = 0; j < adj[u].count; j++) {
        int32_t c = colors[adj[u].nbrs[j]];
        if (c >= 0 && c < MAX_COLORS) used[c] = 1;
    }
    for (int32_t c = 0; c < MAX_COLORS; c++)
        if (!used[c]) return c;
    return MAX_COLORS;
}

/* ---- Load graph ---- */

static int load_graph(uint32_t *offsets, uint32_t *sorted_nbrs, int32_t *init_colors,
                       uint32_t n, uint32_t ne) {
    free_graph();
    nv = n;

    adj = (AdjList *)calloc(nv, sizeof(AdjList));
    colors = (int32_t *)malloc(nv * sizeof(int32_t));
    prev_colors = (int32_t *)malloc(nv * sizeof(int32_t));

    if (!adj || !colors || !prev_colors) {
        fprintf(stderr, "[GC-P12-DPU] FATAL: alloc failed for graph metadata\n");
        return -1;
    }

    memcpy(colors, init_colors, nv * sizeof(int32_t));
    memcpy(prev_colors, init_colors, nv * sizeof(int32_t));

    if (ne > 0) {
        nbr_pool = (uint32_t *)malloc((size_t)ne * sizeof(uint32_t));
        if (!nbr_pool) {
            fprintf(stderr, "[GC-P12-DPU] FATAL: neighbor pool alloc failed (%.1f GB)\n",
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

    printf("[GC-P12-DPU] Memory: adj=%.0f MB, pool=%.0f MB, colors=%.0f MB\n",
           (double)nv * sizeof(AdjList) / (1024.0 * 1024.0),
           (double)ne * 4.0 / (1024.0 * 1024.0),
           (double)nv * 4.0 / (1024.0 * 1024.0));
    return 0;
}

/* ---- Batch structs ---- */

typedef struct { uint32_t src, dst, is_ins; } BatchEdge;
typedef struct { uint32_t node_id; int32_t old_color, new_color; } ColorChange;

/* ---- ProcessCE (Phase 1) ---- */

static void process_ce(BatchEdge *edges, uint32_t num_edges,
                        uint32_t *aff_buf, uint32_t *num_aff,
                        ColorChange *cc_buf, uint32_t *num_cc) {
    uint32_t na = 0, nc = 0;

    /* Find partition: deletions first (is_ins=0), then insertions */
    uint32_t mid = 0;
    for (uint32_t i = 0; i < num_edges; i++)
        if (edges[i].is_ins) { mid = i; break; }
    if (mid == 0 && num_edges > 0 && edges[0].is_ins) mid = 0;
    else if (mid == 0) mid = num_edges;

    /* Deletions */
    for (uint32_t i = 0; i < mid; i++) {
        uint32_t a = edges[i].src, b = edges[i].dst;
        if (a >= nv || b >= nv) continue;

        int32_t ca = colors[a], cb = colors[b];
        uint32_t y = (ca >= cb) ? a : b;
        uint32_t z = (y == a) ? b : a;
        int32_t target_color = colors[z];

        int in_sc = 0;
        for (uint32_t j = 0; j < adj[y].count; j++) {
            uint32_t nbr = adj[y].nbrs[j];
            if (nbr == z) continue;
            if (colors[nbr] == target_color) { in_sc = 1; break; }
        }
        if (!in_sc) {
            int32_t old = colors[y];
            prev_colors[y] = old;
            colors[y] = target_color;
            aff_buf[na++] = y;
            cc_buf[nc].node_id = y;
            cc_buf[nc].old_color = old;
            cc_buf[nc].new_color = target_color;
            nc++;
        }
    }

    /* Insertions */
    for (uint32_t i = mid; i < num_edges; i++) {
        uint32_t a = edges[i].src, b = edges[i].dst;
        if (a >= nv || b >= nv) continue;

        if (colors[a] == colors[b]) {
            uint32_t y = (a > b) ? a : b;
            int32_t old = colors[y];
            int32_t new_c = smallest_available_color(y);
            prev_colors[y] = old;
            colors[y] = new_c;
            aff_buf[na++] = y;
            cc_buf[nc].node_id = y;
            cc_buf[nc].old_color = old;
            cc_buf[nc].new_color = new_c;
            nc++;
        }
    }

    *num_aff = na;
    *num_cc = nc;
}

/* ---- CheckConflict (Phase 2) ---- */

static void check_conflict(uint32_t *aff, uint32_t num_aff,
                             uint32_t *next_aff, uint32_t *num_next,
                             ColorChange *cc_buf, uint32_t *num_cc) {
    uint32_t nn = 0;

    for (uint32_t i = 0; i < num_aff; i++) {
        uint32_t v = aff[i];
        if (v >= nv) continue;
        int32_t cv = colors[v];

        for (uint32_t j = 0; j < adj[v].count; j++) {
            uint32_t u = adj[v].nbrs[j];
            if (colors[u] == cv) {
                uint32_t y = (u > v) ? u : v;
                int32_t old = colors[y];
                prev_colors[y] = old;
                int32_t new_c = smallest_available_color(y);
                colors[y] = new_c;
                next_aff[nn++] = y;
                cc_buf[*num_cc].node_id = y;
                cc_buf[*num_cc].old_color = old;
                cc_buf[*num_cc].new_color = new_c;
                (*num_cc)++;
            }
        }
    }

    *num_next = nn;
}

/* ---- Deduplicate uint32 array ---- */

static int cmp_u32(const void *a, const void *b) {
    uint32_t va = *(const uint32_t *)a, vb = *(const uint32_t *)b;
    return (va > vb) - (va < vb);
}

static uint32_t dedup_u32(uint32_t *arr, uint32_t n) {
    if (n <= 1) return n;
    qsort(arr, n, sizeof(uint32_t), cmp_u32);
    uint32_t w = 0;
    for (uint32_t r = 0; r < n; r++)
        if (r == 0 || arr[r] != arr[r - 1]) arr[w++] = arr[r];
    return w;
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

    printf("[GC-P12-DPU] ProcessCE+CheckConflict Server on port %d.\n", PORT);
    printf("[GC-P12-DPU] UpdateNeighbors (P3) offloaded to host CPU. Press Ctrl+C to stop.\n\n");

    while (1) {
        printf("[GC-P12-DPU] Waiting for host connection...\n");
        client_fd = accept(server_fd, NULL, NULL);
        if (client_fd < 0) continue;
        printf("[GC-P12-DPU] Host connected.\n");

        /* Receive graph */
        uint32_t n, ne;
        if (recv_all(client_fd, &n, 4) < 0 || recv_all(client_fd, &ne, 4) < 0) goto done;

        printf("[GC-P12-DPU] Receiving graph: %u vertices, %u CSR edges...\n", n, ne);

        uint32_t *off = (uint32_t *)malloc((size_t)(n + 1) * sizeof(uint32_t));
        uint32_t *nbrs = (ne > 0) ? (uint32_t *)malloc((size_t)ne * sizeof(uint32_t)) : NULL;
        int32_t *init_colors = (int32_t *)malloc((size_t)n * sizeof(int32_t));

        if (!off || !init_colors || (ne > 0 && !nbrs)) {
            fprintf(stderr, "[GC-P12-DPU] FATAL: recv buffer alloc failed\n");
            free(off); free(nbrs); free(init_colors); goto done;
        }

        if (recv_all(client_fd, off, (size_t)(n + 1) * 4) < 0) { free(off); free(nbrs); free(init_colors); goto done; }
        if (ne > 0 && recv_all(client_fd, nbrs, (size_t)ne * 4) < 0) { free(off); free(nbrs); free(init_colors); goto done; }
        if (recv_all(client_fd, init_colors, (size_t)n * 4) < 0) { free(off); free(nbrs); free(init_colors); goto done; }

        if (load_graph(off, nbrs, init_colors, n, ne) < 0) { free(off); free(nbrs); free(init_colors); goto done; }
        free(off); free(nbrs); free(init_colors);

        printf("[GC-P12-DPU] Graph loaded. Ready for batches.\n");

        /* Scratch buffers */
        uint32_t *aff_buf = (uint32_t *)malloc(n * sizeof(uint32_t));
        uint32_t *aff_buf2 = (uint32_t *)malloc(n * sizeof(uint32_t));
        ColorChange *cc_buf = (ColorChange *)malloc(n * 2 * sizeof(ColorChange));
        if (!aff_buf || !aff_buf2 || !cc_buf) {
            fprintf(stderr, "[GC-P12-DPU] FATAL: scratch alloc failed\n");
            free(aff_buf); free(aff_buf2); free(cc_buf); goto done;
        }

        /* Batch loop */
        int batch_num = 0;
        while (1) {
            uint32_t num_edges;
            if (recv_all(client_fd, &num_edges, 4) < 0) {
                printf("[GC-P12-DPU] Host disconnected.\n"); break;
            }
            if (num_edges == SHUTDOWN_SENTINEL) {
                printf("[GC-P12-DPU] Session complete.\n"); break;
            }

            batch_num++;

            /* Step 1: Receive batch edges */
            BatchEdge *edges = NULL;
            if (num_edges > 0) {
                edges = (BatchEdge *)malloc(num_edges * sizeof(BatchEdge));
                if (!edges) break;
                if (recv_all(client_fd, edges, num_edges * sizeof(BatchEdge)) < 0) { free(edges); break; }
            }

            /* Step 2: Receive color syncs from previous P3 */
            uint32_t num_color_syncs;
            if (recv_all(client_fd, &num_color_syncs, 4) < 0) { free(edges); break; }
            if (num_color_syncs > 0) {
                uint32_t *sync_buf = (uint32_t *)malloc(num_color_syncs * 2 * sizeof(uint32_t));
                if (!sync_buf) { free(edges); break; }
                if (recv_all(client_fd, sync_buf, num_color_syncs * 8) < 0) { free(sync_buf); free(edges); break; }
                for (uint32_t i = 0; i < num_color_syncs; i++) {
                    uint32_t node_id = sync_buf[i * 2];
                    int32_t new_color = (int32_t)sync_buf[i * 2 + 1];
                    if (node_id < nv) { colors[node_id] = new_color; prev_colors[node_id] = new_color; }
                }
                free(sync_buf);
            }

            /* Step 3: Receive edge syncs from previous batch */
            uint32_t num_edge_syncs;
            if (recv_all(client_fd, &num_edge_syncs, 4) < 0) { free(edges); break; }
            if (num_edge_syncs > 0) {
                uint32_t *esync = (uint32_t *)malloc(num_edge_syncs * 3 * sizeof(uint32_t));
                if (!esync) { free(edges); break; }
                if (recv_all(client_fd, esync, num_edge_syncs * 12) < 0) { free(esync); free(edges); break; }
                /* DPU doesn't need to apply edge syncs — host already did topology update
                 * and sent the updated classification (is_ins). But we keep protocol consistent. */
                free(esync);
            }

            if (num_edges == 0) {
                uint32_t zero = 0; uint64_t zero64 = 0;
                send_all(client_fd, &zero, 4);
                send_all(client_fd, &zero, 4);
                send_all(client_fd, &zero, 4);
                send_all(client_fd, &zero64, 8);
                send_all(client_fd, &zero64, 8);
                free(edges);
                continue;
            }

            /* Step 4: Apply topology update (GC: graph update BEFORE coloring) */
            for (uint32_t i = 0; i < num_edges; i++) {
                if (edges[i].src >= nv || edges[i].dst >= nv) continue;
                if (edges[i].is_ins)
                    edge_insert(edges[i].src, edges[i].dst);
                else
                    edge_remove(edges[i].src, edges[i].dst);
            }

            /* Step 5: ProcessCE (Phase 1, timed) */
            struct timeval t0, t1, t2;
            gettimeofday(&t0, NULL);

            uint32_t num_aff = 0, num_cc = 0;
            process_ce(edges, num_edges, aff_buf, &num_aff, cc_buf, &num_cc);

            gettimeofday(&t1, NULL);
            uint64_t p1_us = (t1.tv_sec - t0.tv_sec) * 1000000ULL + (t1.tv_usec - t0.tv_usec);

            /* Step 6: CheckConflict (Phase 2, timed) — loop until no new conflicts */
            num_aff = dedup_u32(aff_buf, num_aff);
            int p2_iters = 0;

            while (num_aff > 0 && p2_iters < 50) {
                uint32_t num_next = 0;
                check_conflict(aff_buf, num_aff, aff_buf2, &num_next, cc_buf, &num_cc);

                /* Swap buffers */
                uint32_t *tmp = aff_buf; aff_buf = aff_buf2; aff_buf2 = tmp;
                num_aff = dedup_u32(aff_buf, num_next);
                p2_iters++;
            }

            gettimeofday(&t2, NULL);
            uint64_t p2_us = (t2.tv_sec - t1.tv_sec) * 1000000ULL + (t2.tv_usec - t1.tv_usec);

            /* Step 7: Send results — affected vertices for P3 (UpdateNeighbors) */
            /* Aff for P3 = vertices that were recolored in P2 (they have prev_color set) */
            if (send_all(client_fd, &num_aff, 4) < 0) { free(edges); break; }
            if (num_aff > 0 && send_all(client_fd, aff_buf, num_aff * 4) < 0) { free(edges); break; }
            if (send_all(client_fd, &num_cc, 4) < 0) { free(edges); break; }
            if (num_cc > 0 && send_all(client_fd, cc_buf, num_cc * sizeof(ColorChange)) < 0) { free(edges); break; }
            uint32_t zero_muts = 0;
            if (send_all(client_fd, &zero_muts, 4) < 0) { free(edges); break; }
            if (send_all(client_fd, &p1_us, 8) < 0) { free(edges); break; }
            if (send_all(client_fd, &p2_us, 8) < 0) { free(edges); break; }

            printf("[GC-P12-DPU] Batch %d: %u edges, p1=%lu us, p2=%lu us (iters=%d), aff=%u, cc=%u\n",
                   batch_num, num_edges, (unsigned long)p1_us, (unsigned long)p2_us,
                   p2_iters, num_aff, num_cc);

            free(edges);
        }

        free(aff_buf);
        free(aff_buf2);
        free(cc_buf);

done:
        close(client_fd);
        printf("[GC-P12-DPU] Ready for next connection.\n\n");
    }

    close(server_fd);
    return 0;
}
