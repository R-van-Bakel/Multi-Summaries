import networkx as nx
from networkx import Graph
import os
from numbers import Number
from matplotlib import pyplot as plt
import sys
import warnings
from summary_loader.loader_functions import get_summary_graph, get_node_intervals, get_local_global_maps, get_local_global_maps_legacy, get_outcome, get_fixed_point, compute_edge_intervals
from summary_loader.test_cases import test_cases

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
    experiment_directory_test = sys.argv[2]  # a directory containing experiment results
    experiment = os.path.split(os.path.abspath(experiment_directory_test))[1]  # e.g. `simple_cycle`
    if len(sys.argv) >= 4 and test_index in {1,2}:
        experiment_directory_control = sys.argv[3]  # a directory containing experiment results (of a verified/correct version of the algorithms)
        experiment_control = os.path.split(os.path.abspath(experiment_directory_control))[1]
        if (experiment != experiment_control):
            warnings.warn(f"\033[91mThe specified test experiment ({experiment}) and control experiment ({experiment_control}) do not seem to match\033[00m")
    verbose = "-v" in sys.argv

    print("Reading from:", experiment_directory_test)
    edge_index, edge_type = get_summary_graph(experiment_directory_test)
    edges = list(map(list, zip(*edge_index)))
    node_intervals = get_node_intervals(experiment_directory_test)
    fixed_point = get_fixed_point(experiment_directory_test, require_fixed=True)
    if fixed_point < 0:  # This test requires that the fixed point has been reached
        sys.exit(1)
    edge_intervals = compute_edge_intervals(edges,node_intervals,fixed_point)
    nodes = set(node for edge in edge_index for node in edge)
    local_to_global_map, global_to_local_map = get_local_global_maps(experiment_directory_test,True)

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

        # Check if these graphs could be isomorphic
        maybe_isomorphic = nx.faster_could_be_isomorphic(G, G_control)
        print("Could be isomorphic:", maybe_isomorphic)#, edge_match=compare_predicate_and_interval))
        if test_index == 2 and maybe_isomorphic:
            # Check if these graphs are isomorphic
            print("Isomorphic:", nx.is_isomorphic(G, G_control, edge_match=compare_predicate_and_interval))
    else:
        sys.exit(1)
