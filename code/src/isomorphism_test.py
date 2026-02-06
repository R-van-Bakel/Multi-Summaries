import networkx as nx
from networkx import Graph
import os
from numbers import Number
import json
from matplotlib import pyplot as plt
import sys

BYTES_PER_ENTITY = 5
BYTES_PER_PREDICATE = 4
BYTES_PER_BLOCK = 4
BYTES_PER_BLOCK_OR_SINGLETON = 5
BYTES_PER_K_TYPE = 2

def get_summary_graph(experiment_directory:str) -> tuple[list[list[int]], list[int]]:
    assert os.path.exists(experiment_directory), "The experiment directory string should refer to a valid (existing) directory"
    graph_binary_file = experiment_directory + "bisimulation/condensed_multi_summary_graph.bin"
    assert os.path.isfile(graph_binary_file), "The graph (binary) file should exists"

    edge_index = [[],[]]
    edge_type = []

    with open(graph_binary_file, "rb") as f:
        while (subject_bytes := f.read(BYTES_PER_BLOCK_OR_SINGLETON)):
            subject_id = int.from_bytes(subject_bytes, "little", signed=True)
            predicate_id = int.from_bytes(f.read(BYTES_PER_PREDICATE), "little", signed=False)
            object_id = int.from_bytes(f.read(BYTES_PER_BLOCK_OR_SINGLETON), "little", signed=True)

            edge_index[0].append(subject_id)
            edge_index[1].append(object_id)
            edge_type.append(predicate_id)
    
    return edge_index, edge_type

def get_node_intervals(experiment_directory:str) -> dict[int,list[int]]:
    assert os.path.exists(experiment_directory), "The experiment directory string should refer to a valid (existing) directory"
    intervals_binary_file = experiment_directory + "bisimulation/condensed_multi_summary_intervals.bin"
    assert os.path.isfile(intervals_binary_file), "The interval (binary) file should exist"

    node_intervals = dict()

    with open(intervals_binary_file, "rb") as f:
        while (node_bytes := f.read(BYTES_PER_BLOCK_OR_SINGLETON)):
            node_id = int.from_bytes(node_bytes, "little", signed=True)
            start_level = int.from_bytes(f.read(BYTES_PER_K_TYPE), "little", signed=False)
            end_level = int.from_bytes(f.read(BYTES_PER_K_TYPE), "little", signed=False)

            node_intervals[node_id] = [start_level,end_level]
    
    return node_intervals

def get_local_global_maps(experiment_directory:str) -> dict[(int,int),int]:
    assert os.path.exists(experiment_directory), "The experiment directory string should refer to a valid (existing) directory"
    local_to_global_map_binary_file = experiment_directory + "bisimulation/condensed_multi_summary_local_global_map.bin"
    assert os.path.isfile(local_to_global_map_binary_file), "The local to global map (binary) file should exist"

    local_to_global_map = dict()
    global_to_local_map =  dict()

    with open(local_to_global_map_binary_file, "rb") as f:
        while (level_bytes := f.read(BYTES_PER_K_TYPE)):
            local_level = int.from_bytes(level_bytes, "little", signed=False)
            local_block_id = int.from_bytes(f.read(BYTES_PER_BLOCK_OR_SINGLETON), "little", signed=True)
            global_block_id = int.from_bytes(f.read(BYTES_PER_BLOCK_OR_SINGLETON), "little", signed=True)

            local_to_global_map[(local_block_id,local_level)] = global_block_id
            if global_block_id in global_to_local_map:
                global_to_local_map[global_block_id].add((local_block_id,local_level))
            else:
                global_to_local_map[global_block_id] = {(local_block_id,local_level)}

    return local_to_global_map, global_to_local_map

def get_local_global_maps_legacy(experiment_directory:str) -> dict[(int,int),int]:
    assert os.path.exists(experiment_directory), "The experiment directory string should refer to a valid (existing) directory"
    local_to_global_map_binary_file = experiment_directory + "bisimulation/condensed_multi_summary_local_global_map.bin"
    assert os.path.isfile(local_to_global_map_binary_file), "The local to global map (binary) file should exist"

    local_to_global_map = dict()
    global_to_local_map =  dict()

    with open(local_to_global_map_binary_file, "rb") as f:
        while (local_level_bytes := f.read(BYTES_PER_K_TYPE)):
            local_level = int.from_bytes(local_level_bytes, "little", signed=False)
            map_size = int.from_bytes(f.read(BYTES_PER_BLOCK_OR_SINGLETON), "little", signed=True)
            for i in range(map_size):
                local_block_id = int.from_bytes(f.read(BYTES_PER_BLOCK_OR_SINGLETON), "little", signed=True)
                global_block_id = int.from_bytes(f.read(BYTES_PER_BLOCK_OR_SINGLETON), "little", signed=True)

                local_to_global_map[(local_block_id,local_level)] = global_block_id
                if global_block_id in global_to_local_map:
                    global_to_local_map[global_block_id].add((local_block_id,local_level))
                else:
                    global_to_local_map[global_block_id] = {(local_block_id,local_level)}

    return local_to_global_map, global_to_local_map
    
def get_outcome(experiment_directory:str, level:int) -> dict[int,set[int]]:
    assert os.path.exists(experiment_directory), "The experiment directory string should refer to a valid (existing) directory"
    outcome_binary_file = experiment_directory + f"bisimulation/outcome_condensed-{level:04d}.bin"
    assert os.path.isfile(outcome_binary_file), "The outcome (binary) file should exist"

    outcome = dict()
    with open(outcome_binary_file, "rb") as f:
        while (block_bytes := f.read(BYTES_PER_BLOCK)):
            block_id = int.from_bytes(block_bytes, "little", signed=False)
            block_size = int.from_bytes(f.read(BYTES_PER_ENTITY), "little", signed=False)

            outcome[block_id] = set()
            for i in range(block_size):
                entity = int.from_bytes(f.read(BYTES_PER_ENTITY), "little", signed=False)
                outcome[block_id].add(entity)

    return outcome

def get_fixed_point(experiment_directory:str, require_fixed:str=False) -> int:
    assert os.path.exists(experiment_directory), "The experiment directory string should refer to a valid (existing) directory"
    stats_json_file = experiment_directory + "ad_hoc_results/graph_stats.json"
    assert os.path.isfile(stats_json_file), "The statistics (json) file should exist"

    with open(stats_json_file, "r") as f_stats:
        graph_stats = json.load(f_stats)
    
    if require_fixed and not graph_stats["Fixed point"]:
        return -1
    return graph_stats["Final depth"]

def compute_edge_intervals(edges:list[int], node_intervals:dict[int,list[int]], fixed_point:int=-1) -> list[list[int]]:
    edge_intervals = []
    for edge in edges:
        s1, e1 = node_intervals[edge[0]]
        s2, e2 = node_intervals[edge[1]]
        if e1 == e2 == fixed_point:  # This is the special case where the fixed point is reached (i.e. the actual end time is infinity)
            edge_intervals.append([max(s1,s2+1),fixed_point+1])
            continue
        edge_intervals.append([max(s1,s2+1),min(e1,e2+1)])
    return edge_intervals


def compare_predicate(D1:dict[str,Number], D2:dict[str,Number]) -> bool:
    """_summary_

    Args:
        D1 (dict[str,Number]): A dictionary containing the `predicate` edge attribute for the first edge
        D2 (dict[str,Number]): A dictionary containing the `predicate` edge attribute for the second edge

    Returns:
        bool: A boolean value indicating whether both edges have the same `predicate` label
    """    
    return D1["predicates"] == D2["predicates"]

def compare_predicate_and_interval(D1:dict[str,Number], D2:dict[str,Number]) -> bool:
    """_summary_

    Args:
        D1 (dict[str,Number]): A dictionary containing the `labels` tuple with the `predicate`, `start_time` and `end_time` edge attributes for the first edge
        D2 (dict[str,Number]): A dictionary containing the `labels` tuple with the `predicate`, `start_time` and `end_time` edge attributes for the second edge

    Returns:
        bool: A boolean value indicating whether both edges have the same `(predicate,start_time,end_time)` label (i.e. the same predicate and time interval)
    """    
    return D1["predicates"] == D2["predicates"] and D1["time_interval"] == D2["time_interval"]

def add_predicate_labels(nx_graph:Graph, edges:list[list[int]], edge_type:list[int]) -> None:
    for i in range(len(edge_type)):
        v1 = edges[i][0]
        v2 = edges[i][1]
        if "predicates" in nx_graph[v1][v2]:
            nx_graph[v1][v2]["predicates"].add(edge_type[i])
        else:
            nx_graph[v1][v2]["predicates"] = {edge_type[i]}

def add_predicate_and_interval_labels(nx_graph:Graph, edges:list[list[int]], edge_type:list[int], edge_intervals:list[list[int]]) -> None:
    for i in range(len(edge_type)):
        v1 = edges[i][0]
        v2 = edges[i][1]
        if "predicates" in nx_graph[v1][v2]:
            nx_graph[v1][v2]["predicates"].add(edge_type[i])
        else:
            nx_graph[v1][v2]["predicates"] = {edge_type[i]}
        if "time_interval" not in nx_graph[v1][v2]:
            nx_graph[v1][v2]["time_interval"] = (edge_intervals[i][0], edge_intervals[i][1])

if __name__ == "__main__":
    test_index = int(sys.argv[1])  # e.g. `2`
    experiment = sys.argv[2]  # e.g. `simple_cycle`
    experiment_directory_test = sys.argv[3] + experiment + "/"  # a directory containing experiment results
    if len(sys.argv) >= 5:
        experiment_directory_control = sys.argv[4] + experiment + "/"  # a directory containing experiment results (of a verified/correct version of the algorithms)
    verbose = False
    if "-v" in sys.argv:
        verbose = True

    # >>> TEST CASES >>>
    simple_edge = {
        "nodes": {1, -2, -1},
        "edges": [[-2, -1], [-2, 1]],
        "edge types": [1, 1],
        "edge intervals": [[2, 2], [1, 1]]
    }
    simple_self_loop = {  # This assumes a rogue disconnected node (for literals)
        "nodes": {1, -2},
        "edges": [[-2, -2], [-2, 1]],
        "edge types": [1, 1],
        "edge intervals": [[2, 2], [1, 1]]
    }
    simple_small_cycle = {
        "nodes": {1, 2},
        "edges": [[1, 2], [1, 1]],
        "edge types": [1, 1],
        "edge intervals": [[1, 1], [2, 2]]
    }
    simple_chain = {
        "nodes": {1, 2, 3, 4, 5, 6, 7, 8, -1, -9, -8, -7, -6, -5, -4, -3, -2},
        "edges": [[-2, -3], [-2, 1], [-4, -5], [-5, -6], [-7, -8], [2, 3], [4, 5], [5, 6], [7, 8], [-3, -4], [-6, -7], [-8, -9], [-9, -1], [1, 2], [3, 4], [6, 7]],
        "edge types": [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
        "edge intervals": [[9, 9], [8, 8], [7, 9], [6, 9], [4, 9], [6, 6], [4, 4], [3, 3], [1, 1], [8, 9], [5, 9], [3, 9], [2, 9], [7, 7], [5, 5], [2, 2]]
    }
    simple_chain_fork = {
        "nodes": {1, 2, 3, 4, 5, 6, 7, 8, 9, -9, -8, -7, -6, -5, -4, -3, -2},
        "edges": [[-2, 2], [-2, -3], [-4, -5], [-5, -6], [-7, -8], [2, 3], [4, 5], [5, 6], [7, 8], [-3, -4], [-6, -7], [-8, -9], [-9, 1], [3, 4], [6, 7], [8, 9]],
        "edge types": [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
        "edge intervals": [[8, 8], [9, 9], [7, 9], [6, 9], [4, 9], [7, 7], [5, 5], [4, 4], [2, 2], [8, 9], [5, 9], [3, 9], [2, 9], [6, 6], [3, 3], [1, 1]]
    }
    simple_cycle = {
        "nodes": {1, 2, -1},
        "edges": [[1, 2], [1, 1], [1, -1]],
        "edge types": [1, 1, 1],
        "edge intervals": [[1, 1], [2, 2], [2, 2]]
    }
    simple_cycles = {
        "nodes": {1, 2, 3, 4, 5, 6, -1, -8, -6, -5, -2},
        "edges": [[-2, -5], [-2, 2], [-2, 4], [-2, 5], [-2, 1], [-2, -1], [-2, 3], [-5, 2], [-5, -6], [2, 3], [4, 5], [5, 6], [-6, 1], [1, -8], [-8, -2], [3, 4]],
        "edge types": [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
        "edge intervals": [[6, 6], [5, 5], [3, 3], [2, 2], [5, 6], [2, 6], [4, 4], [5, 5], [6, 6], [4, 4], [2, 2], [1, 1], [5, 6], [4, 6], [3, 6], [3, 3]]
    }
    simple_dag = {
        "nodes": {1, 2, 3, 4, 5, 6, 7, 8, -9, -7, -1},
        "edges": [[2, 4], [2, 3], [-7, -9], [4, 5], [5, 6], [7, 8], [3, 1], [1, -7], [-9, -1], [6, 7]],
        "edge types": [1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
        "edge intervals": [[5, 5], [6, 6], [3, 6], [4, 4], [3, 3], [1, 1], [5, 6], [4, 6], [2, 6], [2, 2]]
    }
    heterogeneous_cycle = {
        "nodes": {1, 2, 3, -1},
        "edges": [[2, 1], [2, 3], [1, 2], [1, -1], [1, 3]],
        "edge types": [1, 1, 2, 2, 2],
        "edge intervals": [[2, 2], [1, 1], [2, 2], [2, 2], [1, 1]]
    }
    heterogeneous_hubs = {
        "nodes": {1, 2, 3, -4},
        "edges": [[-4, 2], [-4, 3], [1, -4], [1, 3], [1, 1], [1, 3], [1, 2], [1, 3]],
        "edge types": [1, 1, 2, 2, 1, 1, 3, 3],
        "edge intervals": [[2, 2], [1, 1], [2, 2], [1, 1], [2, 2], [1, 1], [2, 2], [1, 1]]
    }
    multi_block = {
        "nodes": {1, 2, 3, 4, 5, 6, 7, 8, 9, -14, -12, -8, -3, -1},
        "edges": [[4, -3], [4, 6], [2, -8], [-12, 5], [5, -1], [7, 8], [7, 9], [1, -3], [1, 6], [3, -8], [-3, -12], [-3, 5], [-3, 6], [-8, -12], [-8, 7], [-8, -14], [-8, 6], [-8, 8], [-14, 5], [6, 8], [8, 9]],
        "edge types": [1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 1, 1, 1, 1, 1, 1, 1, 1, 2, 1, 1],
        "edge intervals": [[4, 4], [3, 3], [3, 4], [3, 4], [2, 4], [2, 2], [1, 1], [4, 4], [3, 3], [3, 4], [4, 4], [3, 4], [3, 3], [4, 4], [2, 3], [4, 4], [3, 3], [2, 2], [3, 4], [2, 2], [1, 1]]
    }
    multi_block_tree = {
        "nodes": {1, 2, 3, 4, -6, -3, -1},
        "edges": [[2, -3], [1, -6], [-3, 4], [-3, -1], [-6, 4], [-6, -1], [3, 4]],
        "edge types": [1, 1, 2, 2, 3, 3, 1],
        "edge intervals": [[2, 3], [2, 3], [1, 1], [2, 3], [1, 1], [2, 3], [1, 1]]
    }
    toxic = {
        "nodes": {1, 2, 3, 4, 5, 6, 7, 8, 9},
        "edges":          [[2, 5], [2, 5], [5, 4], [5, 4], [7, 8], [7, 8], [3, 1], [3, 6], [3, 1], [3, 6], [1, 2], [1, 2], [6, 7], [6, 7], [8, 9], [8, 9]],
        "edge types": [2, 1, 2, 1, 2, 1, 2, 2, 1, 1, 2, 1, 2, 1, 2, 1],
        "edge intervals": [[3, 5], [3, 5], [2, 5], [2, 5], [2, 2], [2, 2], [5, 5], [4, 4], [5, 5], [4, 4], [4, 5], [4, 5], [3, 3], [3, 3], [1, 1], [1, 1]]
    }
    DMAD = {
        "nodes": {1, 2, 3, 4, 5, 6, 7, 8, 9},
        "edges": [[4, 7], [4, 6], [5, 2], [5, 9], [5, 3], [5, 1], [5, 8], [5, 9], [2, 5], [2, 9], [7, 8], [6, 4], [6, 7], [6, 3], [3, 5], [3, 7], [3, 6], [3, 8], [1, 5], [1, 9], [1, 1], [1, 9], [8, 9]],
        "edge types": [1, 1, 2, 2, 1, 1, 1, 1, 2, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 3, 3, 1],
        "edge intervals": [[3, 3], [4, 4], [2, 4], [1, 1], [3, 4], [2, 4], [2, 2], [1, 1], [2, 4], [1, 1], [2, 2], [4, 4], [3, 3], [3, 4], [2, 4], [3, 3], [4, 4], [2, 2], [2, 4], [1, 1], [2, 4], [1, 1], [1, 1]]
    }
    rdf_type_chain = {  # This assumes we split at level 0 for RDF-type
        "nodes": {1, 2, -4, -3, -2},
        "edges": [[-2, 1], [-2, 2], [-2, -3], [-4, 1], [-3, 1], [-3, -4]],
        "edge types": [0, 1, 1, 0, 0, 1],
        "edge intervals": [[1, 2], [1, 1], [2, 2], [1, 2], [1, 2], [1, 2]]
    }
    rdf_type_cycle = {  # This assumes we split at level 0 for RDF-type
        "nodes": {1, -4, -3, -2},
        "edges": [[-2, 1], [-2, -3], [-4, 1], [-4, -2], [-3, 1], [-3, -4]],
        "edge types": [0, 1, 0, 1, 0, 1],
        "edge intervals": [[1, 1], [1, 1], [1, 1], [1, 1], [1, 1], [1, 1]]
    }
    test_cases = {"simple_edge": simple_edge, "simple_self_loop": simple_self_loop, "simple_small_cycle": simple_small_cycle, "simple_chain": simple_chain,
                  "simple_chain_fork": simple_chain_fork, "simple_cycle": simple_cycle, "simple_cycles": simple_cycles, "simple_dag": simple_dag, "heterogeneous_cycle": heterogeneous_cycle,
                  "heterogeneous_hubs": heterogeneous_hubs, "multi_block": multi_block, "multi_block_tree": multi_block_tree, "toxic": toxic, "DMAD": DMAD, "rdf_type_chain": rdf_type_chain,
                  "rdf_type_cycle": rdf_type_cycle}
    # <<< TEST CASES <<<

    print("Reading from:", experiment_directory_test)
    edge_index, edge_type = get_summary_graph(experiment_directory_test)
    edges = list(map(list, zip(*edge_index)))
    node_intervals = get_node_intervals(experiment_directory_test)
    fixed_point = get_fixed_point(experiment_directory_test, require_fixed=True)
    if fixed_point < 0:  # This test requires that the fixed point has been reached
        sys.exit(1)
    edge_intervals = compute_edge_intervals(edges,node_intervals,fixed_point)
    nodes = set(node for edge in edge_index for node in edge)
    local_to_global_map, global_to_local_map = get_local_global_maps(experiment_directory_test)

    # Print some statistics on the test graph
    if verbose:
        print("nodes:         ", nodes)
        print("node intervals:", node_intervals)
        print("edges:         ", edges)
        print("edge types:    ", edge_type)
        print("edge intervals:", edge_intervals)

    # The graph we read from disk
    G = nx.DiGraph()
    G.add_nodes_from(nodes)
    G.add_edges_from(edges)
    add_predicate_and_interval_labels(G, edges, edge_type, edge_intervals)
    print("Number of test nodes:", G.number_of_nodes())
    print("Number of test edges:", G.number_of_edges())
    print()

    if test_index == 0:
        # The test graph we compare against
        G_verified = nx.DiGraph()
        G_verified.add_nodes_from(test_cases[experiment]["nodes"])
        G_verified.add_edges_from(test_cases[experiment]["edges"])
        add_predicate_and_interval_labels(G_verified, test_cases[experiment]["edges"], test_cases[experiment]["edge types"], test_cases[experiment]["edge intervals"])

        # Check if these graphs are isomorphic
        print("Isomorphic:", nx.is_isomorphic(G, G_verified, edge_match=compare_predicate_and_interval))

        nx.draw(G, with_labels=True)
        plt.show()
    elif test_index in {1,2}:
        edge_index_control, edge_type_control = get_summary_graph(experiment_directory_control)
        edges_control = list(map(list, zip(*edge_index_control)))
        node_intervals_control = get_node_intervals(experiment_directory_control)
        fixed_point_control = get_fixed_point(experiment_directory_control)
        edge_intervals_control = compute_edge_intervals(edges_control,node_intervals_control,fixed_point_control)
        nodes_control = set(node for edge in edge_index_control for node in edge)
        local_to_global_map_control, global_to_local_map_control = get_local_global_maps_legacy(experiment_directory_control)

        # statistics on the control graph
        if verbose:
            print("CONTROL results:")
            print("nodes:         ", nodes_control)
            print("node intervals:", node_intervals_control)
            print("edges:         ", edges_control)
            print("edge types:    ", edge_type_control)
            print("edge intervals:", edge_intervals_control)

        G_control = nx.DiGraph()
        G_control.add_nodes_from(nodes_control)
        G_control.add_edges_from(edges_control)
        add_predicate_and_interval_labels(G_control, edges_control, edge_type_control, edge_intervals_control)
        print("Number of control nodes:", G_control.number_of_nodes())
        print("Number of control edges:", G_control.number_of_edges())
        print()

        if test_index == 1:
            # Check if these graphs could be isomorphic
            print("Could be isomorphic:", nx.faster_could_be_isomorphic(G, G_control))#, edge_match=compare_predicate_and_interval))
        elif test_index == 2:
            # Check if these graphs are isomorphic
            print("Isomorphic:", nx.is_isomorphic(G, G_control, edge_match=compare_predicate_and_interval))
    else:
        sys.exit(1)
