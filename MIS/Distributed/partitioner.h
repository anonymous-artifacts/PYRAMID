/*
 * partitioner.h - METIS graph partitioning + 2-hop ghost halo extraction
 */

#pragma once

#include "protocol.h"
#include <vector>
#include <cstdint>
#include <string>

struct PartitionResult {
    std::vector<int32_t> part;   /* part[v] = partition ID for vertex v */
    int num_parts;
    int64_t edge_cut;
};

/* Partition the graph into K parts using METIS */
PartitionResult partition_graph(
    uint32_t nv,
    const std::vector<std::vector<uint32_t>> &adj,
    int K
);

/* Extract subgraph for partition p with 2-hop ghost halo */
SubGraph extract_subgraph(
    uint32_t nv,
    const std::vector<std::vector<uint32_t>> &adj,
    const std::vector<uint8_t> &membership,
    const std::vector<int32_t> &part,
    int p,
    size_t max_ghost = 2000000  /* cap ghost halo for power-law graphs */
);
