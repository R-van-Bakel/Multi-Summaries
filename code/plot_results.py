from matplotlib import pyplot as plt
from matplotlib import gridspec as grid_spec
from math import log10
import os
import json

def plot(results_list, singleton_counts=None) -> None:
    for i, results in enumerate(results_list):
        fig, ax = plt.subplots()
        plot_data = [result for result in results[1]["New block sizes"].values()]
        if singleton_counts is not None:
            plot_data += [1 for _ in range(singleton_counts[i])]
        ax.hist(plot_data, bins=100)
        fig.tight_layout()
        if singleton_counts == None:
            fig.savefig(f"./hist-{i+1:04d}.svg")
        else:
            fig.savefig(f"./hist_singletons-{i+1:04d}.svg")
        plt.close()

def log_plot(results_list, singleton_counts=None) -> None:
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
            fig.savefig(f"./log_hist-{i+1:04d}.svg")
        else:
            fig.savefig(f"./log_hist_singletons-{i+1:04d}.svg")
        plt.close()

def plot_with_singletons(results_list) -> None:
    new_singleton_counts = [results_list[0][0]["Singleton count"]]
    for results in results_list[1:]:
        new_singleton_counts.append(results[0]["Singleton count"]-new_singleton_counts[-1])
    plot(results_list, new_singleton_counts)

def log_plot_with_singletons(results_list) -> None:
    new_singleton_counts = [results_list[0][0]["Singleton count"]]
    for results in results_list[1:]:
        new_singleton_counts.append(results[0]["Singleton count"]-new_singleton_counts[-1])
    log_plot(results_list, new_singleton_counts)

if __name__ == "__main__":

    # data_set = "BSBM100M"
    data_set = "BSBM-40000"
    # data_set = "iteration9"
    # data_set = "linkedpaperswithcode_2023-12-19"

    ad_hoc_path = f"./output/{data_set}/ad_hoc_results/"
    post_hoc_path = f"./output/{data_set}/post_hoc_results/"

    results = []

    i = 0
    while os.path.exists(ad_hoc_path + f"statistics_condensed-{i+1:04d}.json") and os.path.exists(post_hoc_path + f"statistics_condensed-{i+1:04d}.json"):
        results.append([])
        with open(ad_hoc_path + f"statistics_condensed-{i+1:04d}.json") as f:
            data = json.load(f)
        results[i].append(data)
        with open(post_hoc_path + f"statistics_condensed-{i+1:04d}.json") as f:
            data = json.load(f)
        results[i].append(data)
        i += 1

        # Distribution of new blocks (normal and log scale)
        plot(results)
        log_plot(results)

        # Distribution of new blocks, including new singletons (normal and log scale)
        plot_with_singletons(results)
        log_plot_with_singletons(results)