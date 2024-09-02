from matplotlib import pyplot as plt
# from matplotlib import gridspec as grid_spec
from math import log10
import sys
import os
import json

def plot(results_list, experiment_directory, singleton_counts=None) -> None:
    for i, results in enumerate(results_list):
        fig, ax = plt.subplots()
        plot_data = [result for result in results[1]["New block sizes"].values()]
        if singleton_counts is not None:
            plot_data += [1 for _ in range(singleton_counts[i])]
        ax.hist(plot_data, bins=100)
        fig.tight_layout()
        if singleton_counts == None:
            fig.savefig(experiment_directory + f"plots/outcome_block_count-{i+1:04d}.svg")
        else:
            fig.savefig(experiment_directory + f"plots/outcome_block_count_with_singletons-{i+1:04d}.svg")
        plt.close()

def log_plot(results_list, experiment_directory, singleton_counts=None) -> None:
    for i, results in enumerate(results_list):
        fig, ax = plt.subplots()
        plot_data = [log10(result) for result in results[1]["New block sizes"].values()]
        if singleton_counts is not None:
            plot_data += [1 for _ in range(singleton_counts[i])]
        ax.hist(plot_data, bins=100)
        labels = [f"{tick:.1E}" for tick in 10**ax.get_xticks()]
        ax.set_xticks(ax.get_xticks(), labels=labels, rotation=45)
        ax.set_yscale('log')
        fig.tight_layout()
        if singleton_counts == None:
            fig.savefig(experiment_directory + f"plots/log_outcome_block_count-{i+1:04d}.svg")
        else:
            fig.savefig(experiment_directory + f"plots/log_outcome_block_count_with_singletons-{i+1:04d}.svg")
        plt.close()

def plot_with_singletons(results_list, experiment_directory) -> None:
    new_singleton_counts = [results_list[0][0]["Singleton count"]]
    for results in results_list[1:]:
        new_singleton_counts.append(results[0]["Singleton count"]-new_singleton_counts[-1])
    plot(results_list, experiment_directory, new_singleton_counts)

def log_plot_with_singletons(results_list, experiment_directory) -> None:
    new_singleton_counts = [results_list[0][0]["Singleton count"]]
    for results in results_list[1:]:
        new_singleton_counts.append(results[0]["Singleton count"]-new_singleton_counts[-1])
    log_plot(results_list, experiment_directory, new_singleton_counts)

if __name__ == "__main__":
    assert len(sys.argv) == 3, "Please provide 1) the experiment directory, 2) the level of k"
    assert os.path.exists(sys.argv[1]), "The first argument should be a valid path to a directory"
    experiment_directory = sys.argv[1]

    try:
        k = int(sys.argv[2])
    except ValueError:
        raise AssertionError("The second argument should be an integer")
    assert k == -1 or k >= 1, "The second argument should either be -1 (for getting all summary graphs) or larger than / equal to 1 (for getting a fixed number of summary graphs)"

    BIN_COUNT = 100

    ad_hoc_path = experiment_directory + "ad_hoc_results/"
    post_hoc_path = experiment_directory + "post_hoc_results/"

    if k == -1:  # In this case we will create the summary graphs for all outcomes
        with open(experiment_directory + "ad_hoc_results/graph_stats.json") as f:
            graph_stats = json.load(f)
            k = graph_stats["Final depth"]

    plot_directory = experiment_directory + "plots/"

    if not os.path.isdir(plot_directory):
        os.makedirs(plot_directory)

    results = []

    for i in range(1,k+1):
        results.append([])
        with open(ad_hoc_path + f"statistics_condensed-{i:04d}.json") as f:
            data = json.load(f)
        results[i-1].append(data)
        with open(post_hoc_path + f"statistics_condensed-{i:04d}.json") as f:
            data = json.load(f)
        results[i-1].append(data)

        # Distribution of new blocks (normal and log scale)
        plot(results, experiment_directory)
        log_plot(results, experiment_directory)

        # Distribution of new blocks, including new singletons (normal and log scale)
        plot_with_singletons(results, experiment_directory)
        log_plot_with_singletons(results, experiment_directory)