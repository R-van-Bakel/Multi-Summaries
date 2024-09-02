from python_binary_loader import get_summary_graph
import matplotlib.pyplot as plt
from collections import Counter
import sys
import os
import json

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

    if k == -1:  # In this case we will create the summary graphs for all outcomes
        with open(experiment_directory + "ad_hoc_results/graph_stats.json") as f:
            graph_stats = json.load(f)
            k = graph_stats["Final depth"]

    plot_directory = experiment_directory + "plots/"

    if not os.path.isdir(plot_directory):
        os.makedirs(plot_directory)

    for i in range(1,k+1):
        edge_index, edge_type = get_summary_graph(experiment_directory, i)
        out_counter = Counter(edge_index[0])
        in_counter = Counter(edge_index[1])
        
        # Plot the out degrees
        plt.hist(out_counter.values(), BIN_COUNT)

        # Label the axes
        plt.xlabel("Degree")
        plt.ylabel("# summary nodes with given degree")

        # Change the y-axis to whole numbers
        current_values = plt.gca().get_yticks()
        plt.gca().set_yticks(current_values)
        plt.gca().set_yticklabels(['{:.0f}'.format(x) for x in current_values])

        plt.savefig(plot_directory + f"out_degrees-{i:04}.svg")
        plt.close()

        # Plot the log out degrees
        plt.hist(out_counter.values(), BIN_COUNT)
        plt.yscale('log')

        # Label the axes
        plt.xlabel("Degree")
        plt.ylabel("log # summary nodes with given degree")

        # Change the y-axis to whole numbers
        current_values = plt.gca().get_yticks()
        plt.gca().set_yticks(current_values)
        plt.gca().set_yticklabels(['{:.0f}'.format(x) for x in current_values])

        plt.savefig(plot_directory + f"log_out_degrees-{i:04}.svg")
        plt.close()



        # Plot the in degrees
        plt.hist(in_counter.values(), BIN_COUNT)

        # Label the axes
        plt.xlabel("Degree")
        plt.ylabel("# summary nodes with given degree")

        # Change the y-axis to whole numbers
        current_values = plt.gca().get_yticks()
        plt.gca().set_yticks(current_values)
        plt.gca().set_yticklabels(['{:.0f}'.format(x) for x in current_values])

        plt.savefig(plot_directory + f"in_degrees-{i:04}.svg")
        plt.close()



        # Plot the log in degrees
        plt.hist(in_counter.values(), BIN_COUNT)
        plt.yscale('log')

        # Label the axes
        plt.xlabel("Degree")
        plt.ylabel("log # summary nodes with given degree")

        # Change the y-axis to whole numbers
        current_values = plt.gca().get_yticks()
        plt.gca().set_yticks(current_values)
        plt.gca().set_yticklabels(['{:.0f}'.format(x) for x in current_values])

        plt.savefig(plot_directory + f"log_in_degrees-{i:04}.svg")
        plt.close()