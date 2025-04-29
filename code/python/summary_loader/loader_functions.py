import os
import json
from collections import Counter

BYTES_PER_ENTITY = 5
BYTES_PER_PREDICATE = 4
BYTES_PER_BLOCK = 4
BYTES_PER_BLOCK_OR_SINGLETON = 5
BYTES_PER_K_TYPE = 2


def get_summary_graph(experiment_directory: str) -> tuple[list[list[int]], list[int]]:
    assert os.path.exists(
        experiment_directory
    ), "The experiment directory string should refer to a valid (existing) directory"
    graph_binary_file = (
        experiment_directory + "bisimulation/condensed_multi_summary_graph.bin"
    )
    assert os.path.isfile(graph_binary_file), "The graph (binary) file should exists"

    edge_index = [[], []]
    edge_type = []

    with open(graph_binary_file, "rb") as f:
        while subject_bytes := f.read(BYTES_PER_BLOCK_OR_SINGLETON):
            subject_id = int.from_bytes(subject_bytes, "little", signed=True)
            predicate_id = int.from_bytes(
                f.read(BYTES_PER_PREDICATE), "little", signed=False
            )
            object_id = int.from_bytes(
                f.read(BYTES_PER_BLOCK_OR_SINGLETON), "little", signed=True
            )

            edge_index[0].append(subject_id)
            edge_index[1].append(object_id)
            edge_type.append(predicate_id)

    return edge_index, edge_type


def get_node_intervals(experiment_directory: str) -> dict[int, tuple[int,int]]:
    assert os.path.exists(
        experiment_directory
    ), "The experiment directory string should refer to a valid (existing) directory"
    intervals_binary_file = (
        experiment_directory + "bisimulation/condensed_multi_summary_intervals.bin"
    )
    assert os.path.isfile(
        intervals_binary_file
    ), "The interval (binary) file should exist"

    node_intervals = dict()

    with open(intervals_binary_file, "rb") as f:
        while node_bytes := f.read(BYTES_PER_BLOCK_OR_SINGLETON):
            node_id = int.from_bytes(node_bytes, "little", signed=True)
            start_level = int.from_bytes(
                f.read(BYTES_PER_K_TYPE), "little", signed=False
            )
            end_level = int.from_bytes(f.read(BYTES_PER_K_TYPE), "little", signed=False)

            node_intervals[node_id] = (start_level, end_level)

    return node_intervals


def get_local_global_maps(
    experiment_directory: str,
    include_inverted_index=False,
) -> tuple[dict[tuple[int, int], int], dict[int, tuple[int, int]]]:
    assert os.path.exists(
        experiment_directory
    ), "The experiment directory string should refer to a valid (existing) directory"
    local_to_global_map_binary_file = (
        experiment_directory
        + "bisimulation/condensed_multi_summary_local_global_map.bin"
    )
    assert os.path.isfile(
        local_to_global_map_binary_file
    ), "The local to global map (binary) file should exist"

    local_to_global_map = dict()
    global_to_local_map = dict()

    with open(local_to_global_map_binary_file, "rb") as f:
        while level_bytes := f.read(BYTES_PER_K_TYPE):
            local_level = int.from_bytes(level_bytes, "little", signed=False)
            local_block_id = int.from_bytes(
                f.read(BYTES_PER_BLOCK_OR_SINGLETON), "little", signed=True
            )
            global_block_id = int.from_bytes(
                f.read(BYTES_PER_BLOCK_OR_SINGLETON), "little", signed=True
            )

            local_to_global_map[(local_block_id, local_level)] = global_block_id
            if include_inverted_index:
                if global_block_id in global_to_local_map:
                    global_to_local_map[global_block_id].add((local_block_id, local_level))
                else:
                    global_to_local_map[global_block_id] = {(local_block_id, local_level)}

    return local_to_global_map, global_to_local_map


def get_local_global_maps_legacy(experiment_directory: str) -> dict[(int, int), int]:
    assert os.path.exists(
        experiment_directory
    ), "The experiment directory string should refer to a valid (existing) directory"
    local_to_global_map_binary_file = (
        experiment_directory
        + "bisimulation/condensed_multi_summary_local_global_map.bin"
    )
    assert os.path.isfile(
        local_to_global_map_binary_file
    ), "The local to global map (binary) file should exist"

    local_to_global_map = dict()
    global_to_local_map = dict()

    with open(local_to_global_map_binary_file, "rb") as f:
        while local_level_bytes := f.read(BYTES_PER_K_TYPE):
            local_level = int.from_bytes(local_level_bytes, "little", signed=False)
            map_size = int.from_bytes(
                f.read(BYTES_PER_BLOCK_OR_SINGLETON), "little", signed=True
            )
            for i in range(map_size):
                local_block_id = int.from_bytes(
                    f.read(BYTES_PER_BLOCK_OR_SINGLETON), "little", signed=True
                )
                global_block_id = int.from_bytes(
                    f.read(BYTES_PER_BLOCK_OR_SINGLETON), "little", signed=True
                )

                local_to_global_map[(local_block_id, local_level)] = global_block_id
                if global_block_id in global_to_local_map:
                    global_to_local_map[global_block_id].add(
                        (local_block_id, local_level)
                    )
                else:
                    global_to_local_map[global_block_id] = {
                        (local_block_id, local_level)
                    }

    return local_to_global_map, global_to_local_map


def get_outcome(experiment_directory: str, level: int) -> dict[int, set[int]]:
    assert os.path.exists(
        experiment_directory
    ), "The experiment directory string should refer to a valid (existing) directory"
    outcome_binary_file = (
        experiment_directory + f"bisimulation/outcome_condensed-{level:04d}.bin"
    )
    assert os.path.isfile(outcome_binary_file), "The outcome (binary) file should exist"

    outcome = dict()
    with open(outcome_binary_file, "rb") as f:
        while block_bytes := f.read(BYTES_PER_BLOCK):
            block_id = int.from_bytes(block_bytes, "little", signed=False)
            block_size = int.from_bytes(
                f.read(BYTES_PER_ENTITY), "little", signed=False
            )

            outcome[block_id] = set()
            for i in range(block_size):
                entity = int.from_bytes(
                    f.read(BYTES_PER_ENTITY), "little", signed=False
                )
                outcome[block_id].add(entity)

    return outcome


def get_fixed_point(experiment_directory: str, require_fixed: str = False) -> int:
    assert os.path.exists(
        experiment_directory
    ), "The experiment directory string should refer to a valid (existing) directory"
    stats_json_file = experiment_directory + "ad_hoc_results/graph_stats.json"
    assert os.path.isfile(stats_json_file), "The statistics (json) file should exist"

    with open(stats_json_file, "r") as f_stats:
        graph_stats = json.load(f_stats)

    if require_fixed and not graph_stats["Fixed point"]:
        return -1
    return graph_stats["Final depth"]


def compute_edge_intervals(
    edges: list[int], node_intervals: dict[int, list[int]], fixed_point: int = -1
) -> list[list[int]]:
    edge_intervals = []
    for edge in edges:
        s1, e1 = node_intervals[edge[0]]
        s2, e2 = node_intervals[edge[1]]
        if (
            e1 == e2 == fixed_point
        ):  # This is the special case where the fixed point is reached (i.e. the actual end time is infinity)
            edge_intervals.append([max(s1, s2 + 1), fixed_point + 1])
            continue
        edge_intervals.append([max(s1, s2 + 1), min(e1, e2 + 1)])
    return edge_intervals


def get_sizes_and_split_blocks(
    experiment_directory: str, depth: int, typed_start=True
) -> tuple[list[dict[str, Counter[int, int]]], list[dict[int, int]]]:
    assert os.path.exists(
        experiment_directory
    ), "The experiment directory string should refer to a valid (existing) directory"
    statistics = []
    sizes = dict()
    accumulated_sizes = dict()
    start = (
        0 if typed_start else 1
    )  # The untyped start has a trivial universal block at level 0, so it will not be stored explicitly
    split_blocks = []
    for i in range(start, depth + 1):
        outcome_binary_file = (
            experiment_directory + f"bisimulation/outcome_condensed-{i:04d}.bin"
        )
        assert os.path.isfile(
            outcome_binary_file
        ), "The outcome (binary) file should exist"

        if i > start:
            mapping_binary_file = (
                experiment_directory + f"bisimulation/mapping-{i-1:04d}to{i:04d}.bin"
            )
            assert os.path.isfile(
                mapping_binary_file
            ), "The mapping (binary) file should exist"

            split_blocks.append({})

            with open(mapping_binary_file, "rb") as f:
                while merged_bytes := f.read(BYTES_PER_BLOCK):
                    merged_id = int.from_bytes(merged_bytes, "little", signed=False)
                    split_count = int.from_bytes(
                        f.read(BYTES_PER_BLOCK), "little", signed=False
                    )
                    split_blocks[-1][merged_id] = sizes[
                        merged_id
                    ]  # Keep track of which blocks split at each level, and the amounts of singletons contained
                    sizes.pop(merged_id)  # Remove the old unused blocks
                    f.seek(split_count * BYTES_PER_BLOCK, 1)

        with open(outcome_binary_file, "rb") as f:
            while block_bytes := f.read(BYTES_PER_BLOCK):
                block_id = int.from_bytes(block_bytes, "little", signed=False)
                block_size = int.from_bytes(
                    f.read(BYTES_PER_ENTITY), "little", signed=False
                )
                sizes[block_id] = block_size
                accumulated_sizes[(i, block_id)] = block_size
                f.seek(block_size * BYTES_PER_ENTITY, 1)
        statistics.append(
            {
                "Block sizes": Counter(sizes.values()),
                "Block sizes (accumulated)": Counter(accumulated_sizes.values()),
            }
        )

    return statistics, split_blocks


def get_statistics(experiment_directory: str, depth: int, typed_start=True) -> any:
    assert os.path.exists(
        experiment_directory
    ), "The experiment directory string should refer to a valid (existing) directory"
    statistics = []
    start = (
        0 if typed_start else 1
    )  # The untyped start has a trivial universal block at level 0, so it will not be stored explicitly
    for i in range(start, depth + 1):
        stats_json_file = (
            experiment_directory + f"ad_hoc_results/statistics_condensed-{i:04d}.json"
        )
        assert os.path.isfile(
            stats_json_file
        ), "The statistics (json) file should exist"

        with open(stats_json_file, "r") as f_stats:
            bisim_stats = json.load(f_stats)
        statistics.append(bisim_stats)
    return statistics


def get_data_edge_statistics(
    experiment_directory: str, depth: int, typed_start=True
) -> any:
    assert os.path.exists(
        experiment_directory
    ), "The experiment directory string should refer to a valid (existing) directory"
    statistics = []
    start = (
        0 if typed_start else 1
    )  # The untyped start has a trivial universal block at level 0, so it will not be stored explicitly
    for i in range(start, depth):
        stats_json_file = (
            experiment_directory
            + f"ad_hoc_results/data_edges_statistics_condensed-{i+1:04d}to{i:04d}.json"
        )
        assert os.path.isfile(
            stats_json_file
        ), "The statistics (json) file should exist"

        with open(stats_json_file, "r") as f_stats:
            data_edge_stats = json.load(f_stats)
        statistics.append(data_edge_stats)

    # This file only exists if the bisimulation was run up to the fixed point
    fixed_point_stats_json_file = (
        f"ad_hoc_results/data_edges_statistics_condensed-{depth:04d}to{depth:04d}.json"
    )
    if os.path.isfile(fixed_point_stats_json_file):
        with open(fixed_point_stats_json_file, "r") as f_stats:
            data_edge_stats = json.load(f_stats)
        statistics.append(data_edge_stats)

    return statistics


def get_graph_statistics(experiment_directory: str) -> any:
    assert os.path.exists(
        experiment_directory
    ), "The experiment directory string should refer to a valid (existing) directory"
    graph_stats_json_file = experiment_directory + f"ad_hoc_results/graph_stats.json"
    assert os.path.isfile(
        graph_stats_json_file
    ), "The statistics (json) file should exist"
    with open(graph_stats_json_file, "r") as f_stats:
        graph_stats = json.load(f_stats)
    return graph_stats


def get_summary_graph_statistics(experiment_directory: str) -> any:
    assert os.path.exists(
        experiment_directory
    ), "The experiment directory string should refer to a valid (existing) directory"
    summary_graph_stats_json_file = (
        experiment_directory + f"ad_hoc_results/summary_graph_stats.json"
    )
    assert os.path.isfile(
        summary_graph_stats_json_file
    ), "The statistics (json) file should exist"
    with open(summary_graph_stats_json_file, "r") as f_stats:
        summary_graph_stats = json.load(f_stats)
    return summary_graph_stats
