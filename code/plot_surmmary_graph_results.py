from python_binary_loader import get_summary_graph
import matplotlib.pyplot as plt
from collections import Counter
import sys
import os

if __name__ == "__main__":
    assert len(sys.argv) == 3, "Please provide 1) the experiment directory, 2) the level of k"
    assert os.path.exists(sys.argv[1]), "The first argument should be a valid path to a directory"
    assert sys.argv[2].isdigit(), "The second argument should be an integer"

    experiment_directory = sys.argv[1]
    k = int(sys.argv[2])

    edge_index, edge_type = get_summary_graph(experiment_directory, k)
    out_counter = Counter(edge_index[0])
    in_counter = Counter(edge_index[1])

    plot_directory = experiment_directory + "plots/"

    if not os.path.isdir(plot_directory):
        os.makedirs(plot_directory)

    bin_count = 100
    
    # Plot the out degrees
    plt.hist(out_counter.values(), bin_count)
    plt.yscale('log')

    # Label the axes
    plt.xlabel("Degree")
    plt.ylabel("log # summary nodes with given degree")

    # Change the y-axis to whole numbers
    current_values = plt.gca().get_yticks()
    plt.gca().set_yticks(current_values)
    plt.gca().set_yticklabels(['{:.0f}'.format(x) for x in current_values])

    plt.savefig(plot_directory + f"out_degrees-{k:04}.svg")



    # Plot the out degrees
    plt.hist(in_counter.values(), bin_count)
    plt.yscale('log')

    # Label the axes
    plt.xlabel("Degree")
    plt.ylabel("log # summary nodes with given degree")

    # Change the y-axis to whole numbers
    current_values = plt.gca().get_yticks()
    plt.gca().set_yticks(current_values)
    plt.gca().set_yticklabels(['{:.0f}'.format(x) for x in current_values])

    plt.savefig(plot_directory + f"in_degrees-{k:04}.svg")