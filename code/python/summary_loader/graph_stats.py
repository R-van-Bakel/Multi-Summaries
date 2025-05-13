import sys
import os
from matplotlib import pyplot as plt
from matplotlib import gridspec
from collections import Counter
from math import ceil, log10
import numpy as np
from test_kde import (
    generic_universal_kde_via_integral_plot,
    EpanechnikovCDF,
    UniformCDF,
)
from summary_loader.loader_functions import (
    get_sizes_and_split_blocks,
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


def bar_data_from_counter(
    counter: Counter[any, any],
) -> tuple[list[any], list[int], range]:
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
        file_name_base = (
            "statistics_" + key.split("(")[0].lower().replace(" ", "_") 
        )  # Change the key to a more file-friendly format
        file_name_base_svg = file_name_base + ".svg"
        file_name_base_pdf = file_name_base + ".pdf"
    
        # Make (the outside of) the figure transparant
        fig.patch.set_visible(False)

        fig.savefig(result_directory + file_name_base_svg, bbox_inches="tight", pad_inches=0.01)
        fig.savefig(result_directory + file_name_base_pdf, bbox_inches="tight", pad_inches=0.01)


def plot_data_edge_statistics(
    statistics: dict[str, int], result_directory: str
) -> None:
    for key in DATA_EDGE_STATISTICS_KEYS:
        values = [statistic[key] for statistic in statistics]
        fig, ax = plt.subplots()
        ax.plot(values, color="#3300cc")
        ax.set_title(key)
        file_name_base = (
            "data_edge_statistics_"
            + key.split("(")[0].lower().replace(" ", "_")
        )  # Change the key to a more file-friendly format
        file_name_base_svg = file_name_base + ".svg"
        file_name_base_pdf = file_name_base + ".pdf"

        # Make (the outside of) the figure transparant
        fig.patch.set_visible(False)

        fig.savefig(result_directory + file_name_base_svg, bbox_inches="tight", pad_inches=0.01)
        fig.savefig(result_directory + file_name_base_pdf, bbox_inches="tight", pad_inches=0.01)


def plot_block_sizes(
    block_sizes: list[dict[str, Counter[int, int]]],
    result_directory: str,
    sinlgetons: list[int] | None = None,
) -> None:
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
            if sinlgetons is not None:  # If singletons are specified
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
            margin = offset_digits / (maximum_digits_in_bar - offset_digits)
            ax_objs[-1].set_ymargin(margin)

            i += 1
        fig.tight_layout()
        file_name_base = (
            key.replace("(", "").replace(")", "").lower().replace(" ", "_")
        )  # Change the key to a more file-friendly format
        file_name_base_svg = file_name_base + ".svg"
        file_name_base_pdf = file_name_base + ".pdf"

        # Make the outside of the figure transparant
        fig.patch.set_visible(False)

        fig.savefig(result_directory + file_name_base_svg, bbox_inches="tight", pad_inches=0.01)
        fig.savefig(result_directory + file_name_base_pdf, bbox_inches="tight", pad_inches=0.01)


def plot_edges_per_layer(
    edge_intervals: list[list[int]], result_directory: str
) -> None:
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
    file_name_svg = "data_edge_counts.svg"
    file_name_pdf = "data_edge_counts.pdf"

    # Make (the outside of) the figure transparant
    fig.patch.set_visible(False)

    fig.savefig(result_directory + file_name_svg, bbox_inches="tight", pad_inches=0.01)
    fig.savefig(result_directory + file_name_pdf, bbox_inches="tight", pad_inches=0.01)


def plot_split_block_count(
    split_blocks: list[dict[int, int]], result_directory: str
) -> None:
    x = []
    y = []
    for i, blocks in enumerate(split_blocks):
        x.append(i)
        y.append(len(blocks.keys()))

    fig, ax = plt.subplots()
    ax.plot(x, y, color="#3300cc")
    ax.set_title("Number of split blocks per level")
    file_name_svg = "split_block_counts.svg"
    file_name_pdf = "split_block_counts.pdf"

    # Make (the outside of) the figure transparant
    fig.patch.set_visible(False)

    fig.savefig(result_directory + file_name_svg, bbox_inches="tight", pad_inches=0.01)
    fig.savefig(result_directory + file_name_pdf, bbox_inches="tight", pad_inches=0.01)


def plot_split_vertex_count(
    split_blocks: list[set[int]], result_directory: str
) -> None:
    x = []
    y = []
    for i, blocks in enumerate(split_blocks):
        vertex_count = 0
        for count in blocks.values():
            vertex_count += count
        x.append(i)
        y.append(vertex_count)

    fig, ax = plt.subplots()
    ax.plot(x, y, color="#3300cc")
    ax.set_title("Number of vertices in split blocks per level")
    file_name_svg = "split_vertex_counts.svg"
    file_name_pdf = "split_vertex_counts.pdf"

    # Make (the outside of) the figure transparant
    fig.patch.set_visible(False)

    fig.savefig(result_directory + file_name_svg, bbox_inches="tight", pad_inches=0.01)
    fig.savefig(result_directory + file_name_pdf, bbox_inches="tight", pad_inches=0.01)


def plot_split_blocks_and_vertices_and_singletons(
    statistics: dict[str, int],
    split_blocks: list[set[int]],
    result_directory: str,
    log_scale: bool = False,
    # mode: str = "standard",
) -> None:
    x = []
    y_singletons = []
    y_split_blocks = []
    y_split_vertices = []

    for i, statistic in enumerate(statistics):
        x.append(i)
        y_singletons.append(statistic["Singleton count"])

    for blocks in split_blocks:
        vertex_count = 0
        for count in blocks.values():
            vertex_count += count
        y_split_blocks.append(len(blocks.keys()))
        y_split_vertices.append(vertex_count)

    fig, ax = plt.subplots()

    WIDTH = 0.3
    ax.bar(
        [xi-WIDTH for xi in x[:-1]],
        y_split_blocks,
        color="#00cc33",
        width=WIDTH,
        alpha=1,
        label="Splitting Blocks",
    )
    ax.bar(
        [xi+WIDTH for xi in x[:-1]],
        y_split_vertices,
        color="#cc3300",
        width=WIDTH,
        alpha=1,
        label="Vertices in Splitting Blocks",
    )
    ax.bar(
        x,
        y_singletons,
        color="#3300cc",
        width=WIDTH,
        label="Singletons",
    )

    ax.set_xlabel("Bisimulation level")
    ax.set_ylabel("Count")
    ax.legend(loc="upper right")
    ymin, ymax = ax.get_ylim()
    PADDING_FACTOR = 1.3
    if log_scale:
        ax.set_title("Log statistics per level")
        ax.set_yscale("log")
        new_ymax = 10 ** (log10(ymax) * PADDING_FACTOR)
        ax.set_ylim(
            0.1, new_ymax
        )  # Padding to accomodate for the legend and padding to increas visibilty of small values
        new_ymax
        y_ticks = [1]
        highest_tick = 10
        while highest_tick < new_ymax:
            y_ticks.append(highest_tick)
            highest_tick *= 10
        ax.set_yticks(y_ticks)
    else:
        ax.set_title("Statistics per level")
        ax.set_ylim(
            ymin, ymax * PADDING_FACTOR
        )  # Padding to accomodate for the legend and padding to increas visibilty of small values
    file_name_svg = "per_level_statistics.svg"
    file_name_pdf = "per_level_statistics.pdf"

    # Make (the outside of) the figure transparant
    fig.patch.set_visible(False)

    fig.savefig(result_directory + file_name_svg, bbox_inches="tight", pad_inches=0.01)
    fig.savefig(result_directory + file_name_pdf, bbox_inches="tight", pad_inches=0.01)


if __name__ == "__main__":
    experiment_directory = sys.argv[1]
    # bar_chart_mode = sys.argv[2]
    verbose = "-v" in sys.argv

    # bar_chart_mode = (
    #     "standard" if bar_chart_mode == "" else bar_chart_mode
    # )  # Set an empty value to the default of "standard"

    result_directory = experiment_directory + "results/"
    os.makedirs(result_directory, exist_ok=True)

    fixed_point = get_fixed_point(experiment_directory)
    statistics = get_statistics(experiment_directory, fixed_point)
    data_edge_statistics = get_data_edge_statistics(experiment_directory, fixed_point)
    graph_statistics = get_graph_statistics(experiment_directory)
    summary_graph_statistics = get_summary_graph_statistics(experiment_directory)
    block_sizes, split_blocks = get_sizes_and_split_blocks(
        experiment_directory, fixed_point
    )

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
    print()

    print("Plotting statistics")
    plot_statistics(statistics, result_directory)
    print("Plotting data edge statistics")
    plot_data_edge_statistics(data_edge_statistics, result_directory)
    print("Plotting block sizes")
    plot_block_sizes(block_sizes, result_directory)

    edge_index, edge_type = get_summary_graph(experiment_directory)
    edges = list(map(list, zip(*edge_index)))
    node_intervals = get_node_intervals(experiment_directory)
    edge_intervals = compute_edge_intervals(edges, node_intervals, fixed_point)

    print("Plotting edges per layer")
    plot_edges_per_layer(edge_intervals, result_directory)

    print("Plotting split block counts")
    plot_split_block_count(split_blocks, result_directory)

    print("Plotting split vertex counts")
    plot_split_vertex_count(split_blocks, result_directory)

    print("Plotting statistics per level")
    plot_split_blocks_and_vertices_and_singletons(
        statistics, split_blocks, result_directory, True#, bar_chart_mode
    )

    # >>> Plot the block sizes heatmap >>>
    print("Plotting heatmaps")

    # Load in the block sizes at different levels along with their count
    data_points = []
    for level, data in enumerate(block_sizes):
        for size, count in data["Block sizes"].items():
            data_points.append((level, size, count))

    # Add the singletons to the data points
    statistics = get_statistics(experiment_directory, fixed_point)
    for level, per_level_statistics in enumerate(statistics):
        singleton_count = per_level_statistics["Singleton count"]
        if singleton_count > 0:  # We don't have to explicitly encode blocks with a count of 0
            data_points.append((level, 1, per_level_statistics["Singleton count"]))

    # Turn the data points into a nmupy array
    data_points = np.stack(data_points)  # shape = number_of_data_points x 3

    via_integration_kwargs = {
        "resolution": 512,
        "weight_type": "block_based",
        "log_size": True,
        "log_base": 10,
        "log_heatmap": True,
        "mark_smallest":True,
        "clip": 0.00,
        "clip_removes": False,
        "plot_name": "block_sizes_integral_kde_block_based",
    }
    base_scale = 0.5
    base_epsilon = 0.5
    padding = 0.05

    epanechnikov_args = ()
    maximum_size = int(data_points[:, 1].max())
    epsilon = (1 - padding) * base_epsilon / fixed_point
    epanechnikov_kwargs = {
        "scale": base_scale / maximum_size,
        "epsilon": epsilon,
    }

    # Block-based plot
    print("Plotting block-based heatmap")
    generic_universal_kde_via_integral_plot(
        data_points,
        result_directory,
        UniformCDF,
        epanechnikov_args,
        epanechnikov_kwargs,
        fixed_point,
        **via_integration_kwargs,
    )

    # Vertex-based plot
    via_integration_kwargs["weight_type"] = "vertex_based"
    via_integration_kwargs["plot_name"] = "block_sizes_integral_kde_vertex_based"
    print("Plotting vertex-based heatmap")
    generic_universal_kde_via_integral_plot(
        data_points,
        result_directory,
        UniformCDF,
        epanechnikov_args,
        epanechnikov_kwargs,
        fixed_point,
        **via_integration_kwargs,
    )
    # <<< Plot the block sizes heatmap <<<