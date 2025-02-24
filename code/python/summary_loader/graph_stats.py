import sys
from summary_loader.loader_functions import get_sizes, get_fixed_point, get_statistics, get_data_edge_statistics, get_graph_statistics, get_summary_graph_statistics

if __name__ == "__main__":
    experiment_directory_test = sys.argv[1]
    verbose = "-v" in sys.argv

    fixed_point = get_fixed_point(experiment_directory_test)
    block_sizes = get_sizes(experiment_directory_test, fixed_point)
    statistics = get_statistics(experiment_directory_test, fixed_point)
    data_edge_statistics = get_data_edge_statistics(experiment_directory_test, fixed_point)
    graph_statistics = get_graph_statistics(experiment_directory_test)
    summary_graph_statistics = get_summary_graph_statistics(experiment_directory_test)

    if verbose:
        print("Statistics:")
        for stats in statistics:
            print(stats)

        print("\nData edge statistics:")
        for stats in data_edge_statistics:
            print(stats)
        
        print("\nGraph statistics:")
        print(graph_statistics)

        print("\nSummary graph statistics:")
        print(summary_graph_statistics)
        
        print("\nBlock sizes:")
        for size_tuple in block_sizes:
            print("Sizes: .................", dict(size_tuple[0]))
            print("Sizes (accumulated): ...", dict(size_tuple[1]))
