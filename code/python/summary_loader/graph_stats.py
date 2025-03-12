import sys
import os
from matplotlib import pyplot as plt
from matplotlib import gridspec
from collections import Counter
from math import ceil, log10
from summary_loader.loader_functions import (
    get_sizes,
    get_fixed_point,
    get_statistics,
    get_data_edge_statistics,
    get_graph_statistics,
    get_summary_graph_statistics,
    get_node_intervals,
    compute_edge_intervals,
    get_summary_graph,
)

STATISTICS_KEYS = {
    "Block count",
    "Singleton count",
    "Accumulated block count",
    "Time taken (ms)",
    "Memory footprint (kB)",
}
DATA_EDGE_STATISTICS_KEYS = {"Time taken (ms)", "Memory footprint (kB)"}
GRAPH_STATISTICS_KEYS = {
    "Vertex count",
    "Edge count",
    "Total time taken (ms)",
    "Maximum memory footprint (kB)",
    "Final depth",
    "Fixed point",
}
SUMMARY_GRAPH_STATISTICS_KEYS = {
    "Vertex count",
    "Edge count",
    "Total time taken (ms)",
    "Maximum memory footprint (kB)",
}
BLOCK_SIZES_KEYS = {"Block sizes", "Block sizes (accumulated)"}

def bar_data_from_counter(counter: Counter[any,any]) -> tuple[list[any],list[int],range]:
    keys = sorted(counter.keys())
    values = [counter[key] for key in keys]
    index_itt = range(len(keys))
    return keys, values, index_itt


def plot_statistics(statistics: dict[str, int], result_directory: str) -> None:
    for key in STATISTICS_KEYS:
        values = [statistic[key] for statistic in statistics]
        fig, ax = plt.subplots()
        ax.plot(values, color="#3300cc")
        ax.set_title(key)
        file_name = (
            "statistics_" + key.split("(")[0].lower().replace(" ", "_") + ".svg"
        )  # Change the key to a more file-friendly format
        fig.savefig(result_directory + file_name)


def plot_data_edge_statistics(
    statistics: dict[str, int], result_directory: str
) -> None:
    for key in DATA_EDGE_STATISTICS_KEYS:
        values = [statistic[key] for statistic in statistics]
        fig, ax = plt.subplots()
        ax.plot(values, color="#3300cc")
        ax.set_title(key)
        file_name = (
            "data_edge_statistics_"
            + key.split("(")[0].lower().replace(" ", "_")
            + ".svg"
        )  # Change the key to a more file-friendly format
        fig.savefig(result_directory + file_name)


def plot_block_sizes(
    block_sizes: list[dict[str, Counter[int, int]]], result_directory: str, sinlgetons: list[int] | None = None) -> None:
    colors = [
        "#0000ff",
        "#3300cc",
        "#660099",
        "#990066",
        "#cc0033",
        "#ff0000",
    ]  # Going from blue to red
    bin_count = 30

    for key in BLOCK_SIZES_KEYS:
        values = [block_sizes_at_level[key] for block_sizes_at_level in block_sizes]
        gs = gridspec.GridSpec(len(values), 1)
        fig = plt.figure(figsize=(8, 1.8 * len(values)))
        i = 0
        ax_objs = []
        for level, size_counter in enumerate(values):
            data = list(size_counter.elements())
            if sinlgetons is not None:  # If singletons are specif
                data += [1 for singleton in sinlgetons]
            ax_objs.append(fig.add_subplot(gs[i : i + 1, 0:]))
            _, _, bars = ax_objs[-1].hist(
                data, bins=bin_count, color=colors[i % 6]
            )  # , density=True)

            # Print the count above the bins
            labels = [int(v) if v > 0 else "" for v in bars.datavalues]
            ax_objs[-1].bar_label(bars, labels=labels, rotation=90)
            largest_bar = max(bars.datavalues)
            digits_largest_bar = int(ceil(log10(largest_bar)))
            offset_digits = digits_largest_bar + 1
            maximum_digits_in_bar = 13
            margin = offset_digits/(maximum_digits_in_bar-offset_digits)
            ax_objs[-1].set_ymargin(margin)
            
            i += 1
        fig.tight_layout()
        file_name = (
            key.replace("(", "").replace(")", "").lower().replace(" ", "_") + ".svg"
        )  # Change the key to a more file-friendly format
        fig.savefig(result_directory + file_name)


def plot_edges_per_layer(edge_intervals: list[list[int]]) -> None:
    edges_per_level_counter = Counter()
    for interval in edge_intervals:
        start_level, end_level = interval
        for i in range(start_level, end_level + 1):
            edges_per_level_counter[i] += 1

    levels, values, _ = bar_data_from_counter(edges_per_level_counter)

    fig, ax = plt.subplots()
    ax.plot(values, color="#3300cc")
    ax.set_xticks(levels)
    ax.set_title("Data edges per level")
    file_name = "data_edge_counts.svg"
    fig.savefig(result_directory + file_name)
    return


if __name__ == "__main__":
    experiment_directory = sys.argv[1]
    verbose = "-v" in sys.argv

    result_directory = experiment_directory + "results/"
    os.makedirs(result_directory, exist_ok=True)

    fixed_point = get_fixed_point(experiment_directory)
    statistics = get_statistics(experiment_directory, fixed_point)
    data_edge_statistics = get_data_edge_statistics(experiment_directory, fixed_point)
    graph_statistics = get_graph_statistics(experiment_directory)
    summary_graph_statistics = get_summary_graph_statistics(experiment_directory)
    block_sizes = get_sizes(experiment_directory, fixed_point)

    plot_statistics(statistics, result_directory)
    plot_data_edge_statistics(data_edge_statistics, result_directory)
    plot_block_sizes(block_sizes, result_directory)

    edge_index, edge_type = get_summary_graph(experiment_directory)
    edges = list(map(list, zip(*edge_index)))
    node_intervals = get_node_intervals(experiment_directory)
    edge_intervals = compute_edge_intervals(edges, node_intervals, fixed_point)

    plot_edges_per_layer(edge_intervals)

    if verbose:
        print("Statistics:")
        for stats in statistics:
            print(stats)

        print("\nData edge statistics:")
        for stats in data_edge_statistics:
            print(stats)

        print("\nBlock sizes:")
        for sizes in block_sizes:
            print(sizes)

    print("\nGraph statistics:")
    print(graph_statistics)

    print("\nSummary graph statistics:")
    print(summary_graph_statistics)
