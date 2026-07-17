/*
 * ghost_manager.h - Ghost node ownership for GC (Graph Coloring)
 *
 * Adapted from MIS version: uses int32_t colors instead of uint8_t membership.
 * Uses DeltaEntry (node_id, new_val) for boundary color deltas.
 */

#pragma once

#include "protocol.h"
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <cstdint>
#include <algorithm>
#include <iostream>

class GhostManager {
public:
    void init(uint32_t owned_count,
              const std::vector<uint32_t> &local_to_global,
              const std::vector<uint32_t> &ghost_nodes,
              const std::vector<int32_t> &ghost_owner,
              const std::vector<uint32_t> &boundary_nodes_global) {
        owned_count_ = owned_count;
        local_to_global_ = local_to_global;

        global_to_local_.reserve(local_to_global.size());
        for (uint32_t i = 0; i < (uint32_t)local_to_global.size(); i++)
            global_to_local_[local_to_global[i]] = i;

        for (size_t i = 0; i < ghost_nodes.size(); i++)
            ghost_owner_map_[ghost_nodes[i]] = ghost_owner[i];

        for (uint32_t gid : boundary_nodes_global) {
            auto it = global_to_local_.find(gid);
            if (it != global_to_local_.end())
                boundary_local_.push_back(it->second);
        }
        boundary_global_set_.insert(boundary_nodes_global.begin(),
                                     boundary_nodes_global.end());
    }

    bool is_owned(uint32_t local_id) const { return local_id < owned_count_; }
    bool is_ghost(uint32_t local_id) const { return local_id >= owned_count_; }

    uint32_t to_global(uint32_t local_id) const { return local_to_global_[local_id]; }

    uint32_t to_local(uint32_t global_id) const {
        auto it = global_to_local_.find(global_id);
        if (it != global_to_local_.end()) return it->second;
        return UINT32_MAX;
    }

    bool has_node(uint32_t global_id) const {
        return global_to_local_.count(global_id) > 0;
    }

    uint32_t owned_count() const { return owned_count_; }
    uint32_t local_nv() const { return (uint32_t)local_to_global_.size(); }

    /* Collect color changes on owned boundary nodes */
    std::vector<DeltaEntry> collect_boundary_color_deltas(
            const std::vector<uint32_t> &changed_local_ids,
            const std::vector<int32_t> &colors) const {
        std::vector<DeltaEntry> deltas;
        for (uint32_t lid : changed_local_ids) {
            if (!is_owned(lid)) continue;
            uint32_t gid = to_global(lid);
            if (boundary_global_set_.count(gid)) {
                deltas.push_back({gid, (uint32_t)colors[lid]});
            }
        }
        return deltas;
    }

    /* Apply color corrections from master */
    void apply_color_corrections(const std::vector<DeltaEntry> &corrections,
                                  std::vector<int32_t> &colors) {
        for (auto &c : corrections) {
            uint32_t lid = to_local(c.node_id);
            if (lid != UINT32_MAX && lid < (uint32_t)colors.size())
                colors[lid] = (int32_t)c.new_val;
        }
    }

    /* Apply ghost color refresh */
    void apply_ghost_color_refresh(const std::vector<DeltaEntry> &deltas,
                                    std::vector<int32_t> &colors) {
        for (auto &d : deltas) {
            uint32_t lid = to_local(d.node_id);
            if (lid != UINT32_MAX && is_ghost(lid) && lid < (uint32_t)colors.size())
                colors[lid] = (int32_t)d.new_val;
        }
    }

private:
    uint32_t owned_count_;
    std::vector<uint32_t> local_to_global_;
    std::unordered_map<uint32_t, uint32_t> global_to_local_;
    std::unordered_map<uint32_t, int32_t> ghost_owner_map_;
    std::vector<uint32_t> boundary_local_;
    std::unordered_set<uint32_t> boundary_global_set_;
};
