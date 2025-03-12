import sys
from math import sqrt, pi, log, floor
import numpy as np
from tqdm import tqdm
from matplotlib import pyplot as plt
from summary_loader.loader_functions import get_fixed_point, get_sizes

# Using the following setting:
# n1 = parameterized_diagonal_multivariate_gaussian(means, standard_deviations)
# n2 = partial(parametric_diagonal_multivariate_gaussian, means=means, standard_deviations=standard_deviations)
# n3 = parametric_diagonal_multivariate_gaussian_speed
# 
# We found that:
# n1_speed > n3_speed = n4_speed

# A function for calculating
def parametric_diagonal_multivariate_gaussian(x: np.ndarray, means: np.ndarray, standard_deviations: np.ndarray) -> np.ndarray:
    dimension = means.size
    variances = np.square(standard_deviations)
    determinant = np.prod(variances)

    normalization_constant = (2 * pi) ** (-dimension/2) * determinant ** (-1/2)
    measure = np.exp((-1/2) * np.square(x - means) * np.reciprocal(variances))
    return normalization_constant * measure

# Create a callable parameterized gaussian. By parameterizing a head of time this function can effectively be reused several times
class parameterized_diagonal_multivariate_gaussian():
    def __init__(self, means: np.ndarray, standard_deviations: np.ndarray) -> None:
        self.means = means
        dimension = means.size
        variances = np.square(standard_deviations)
        determinant = np.prod(variances)

        self.normalization_constant = (2 * pi) ** (-dimension/2) * determinant ** (-1/2)
        self.partial_exponent = (-1/2) * np.reciprocal(variances)
    
    def __call__(self, x: np.ndarray) -> np.ndarray:
        measure = np.exp(np.sum(np.square(x - self.means) * self.partial_exponent, axis=1))
        return self.normalization_constant * measure

def add_minor_log_ticks(positions: np.ndarray, labels: list[str], base: int = 10, resolution: int|None = None) -> tuple[np.ndarray, list[str]]:
    new_positions = []
    new_labels = []
    major_tick_difference = positions[1] - positions[0]
    # minor_tick_offsets = [base**(i/base)*(major_tick_difference/base) for i in range(1, base)]
    minor_tick_offsets = [log(i, base)*major_tick_difference for i in range(2, base)]
    minor_tick_labels = ["" for i in range(base-2)]
    for i in range(len(positions)-1):
        # Add a major tick
        new_positions.append(positions[i])
        # Add the minor ticks
        new_positions += [positions[i] + offset for offset in minor_tick_offsets]
        # Add a major tick label
        new_labels.append(labels[i])
        # Add empty labels for the minor ticks
        new_labels += minor_tick_labels
    # Add the last major tick and label
    new_positions.append(positions[-1])
    new_labels.append(labels[-1])
    # Add the remaining minor ticks (this is important if the plot does not end with a major tick)
    if resolution is not None:
        for offset in minor_tick_offsets:
            minor_tick_position = positions[-1]+offset
            if minor_tick_position > resolution:
                break
            new_positions.append(minor_tick_position)
            new_labels.append("")
    return np.asarray(new_positions), new_labels

if __name__ == "__main__":
    experiment_directory = sys.argv[1]

    # Set the parameters
    # TODO allow these to not be hard-coded
    dimension = 2
    resolution = 512
    log_base = 10
    variance_type = "scott"
    variant_factor = 0.01
    weight_type = "vertex_based"

    # Load in the data
    fixed_point = get_fixed_point(experiment_directory)
    block_sizes = get_sizes(experiment_directory, fixed_point)
    data_points = []
    for level, data in enumerate(block_sizes):
        for size, count in data["Block sizes"].items():
            data_points.append((level,size,count))
    data_points = np.stack(data_points)  # shape = number_of_data_points x 3

    # Each row contains 1) a bisimulation level and 2) a block size
    means = data_points[:, :2].astype(np.float64)

    # The highest size in our input
    maximum_size = int(means[:, 1].max())

    # Normalize the columns
    means[:, 0:1] = means[:, 0:1]/fixed_point
    means[:, 1:2] = means[:, 1:2]/maximum_size

    # Each row is one element indicating the count of occurences of the repsective block size
    # e.g. kernel_weights[0] specifies how many block of size means[1] appear at level means[0]
    kernel_weights = data_points[:, 2:3]

    # We will now set the variance according to either Scott's or Silverman's rule
    # Note that 1) the covariance matrix is effectively a diagonal matrix with the same value sqrt(variance) along its diagonal
    # and 2) we use the exact same variance for each kernel (as opposed to the means which are unique for every kernel)
    # We use `variant_factor` if we want to manually scale the variance
    data_point_count = data_points.shape[0]
    if variance_type == "scott":
        variance = data_point_count**(-1./(dimension+4))
    elif variance_type == "silverman":
        variance = (data_point_count * (dimension + 2) / 4.)**(-1. / (dimension + 4))
    else:
        raise ValueError("`variance_type` should be one of \"scott\" or \"silverman\"")
    variance*=variant_factor
    standard_deviations = np.full(shape=dimension, fill_value=sqrt(variance), dtype=np.float64)

    # Create all the separate Gaussian kernels, using the specified means
    kernels = []
    for i in tqdm(range(data_point_count), desc="Setting up kernels............"):
        kernels.append(parameterized_diagonal_multivariate_gaussian(means[i], standard_deviations))
    
    log_sizes = np.logspace(0, log(maximum_size, log_base), num=resolution, endpoint=True, base=log_base)/maximum_size  # Logarithmically scaled numbers from 0 to 1
    log_levels = (np.logspace(0, log(fixed_point+1, log_base), num=resolution, endpoint=True, base=log_base)-1)/fixed_point  # Logarithmically scaled numbers from 0 to 1
    log_coordinates = np.array(np.meshgrid(log_levels, log_sizes)).T.reshape(-1,2)

    # Calculate the average, weighted kernel
    # The rule `running_mean*(weight/new_weight) + image(current_weight/new_weight)` prevents values from exploding or disappearing
    weight = 0
    running_mean = np.zeros((resolution**2), dtype=np.float64)
    for i in tqdm(range(data_point_count), desc="Calculating average of kernels"):
        image = kernels[i](log_coordinates)
        if weight_type == "block_based":
            current_weight = kernel_weights[i]
        elif weight_type == "vertex_based":
            current_weight = kernel_weights[i]*data_points[i][1]
        else:
            raise ValueError("`weight_type` should be set to one of: \"block_based\" or \"vertex_based\"")
        new_weight = weight + current_weight
        running_mean = running_mean*(weight/new_weight) + image*(current_weight/new_weight)
        weight = new_weight
    kde = running_mean.reshape(resolution,resolution).T

    # Set the x and y ticks differently for small numbers (less that log_base) than for big numbers
    if fixed_point < log_base:
        x_tick_positions = np.asarray([log(i, log_base) for i in range(1,fixed_point+2)])
        x_tick_positions = x_tick_positions*((resolution-1)/float(np.max(x_tick_positions)))
        x_tick_labels = [f"{i}" for i in range(fixed_point+1)]
        plt.xticks(x_tick_positions, x_tick_labels)
    else:
        log_fixed_point = log(fixed_point+1, log_base)
        x_tick_count = int(floor(log_fixed_point)) + 1
        x_tick_positions = np.asarray([i for i in range(x_tick_count)], dtype=np.float64)*(resolution/log_fixed_point)
        x_tick_labels = [f"${log_base}^{i}$" for i in range(x_tick_count)]
        x_tick_positions, x_tick_labels = add_minor_log_ticks(x_tick_positions, x_tick_labels, base=log_base, resolution=resolution)
        plt.xticks(x_tick_positions, x_tick_labels)

    if maximum_size < log_base:
        y_tick_positions = np.asarray([log(i, log_base) for i in range(1,maximum_size+2)])
        y_tick_positions = y_tick_positions*((resolution-1)/float(np.max(y_tick_positions)))
        y_tick_labels = [f"{i}" for i in range(maximum_size+1)]
        plt.yticks(y_tick_positions, y_tick_labels)
    else:
        log_maximum_size = log(maximum_size, log_base)
        y_tick_count = int(floor(log_maximum_size)) + 1
        y_tick_positions = np.asarray([i for i in range(y_tick_count)], dtype=np.float64)*(resolution/log_maximum_size)
        y_tick_labels = [f"${log_base}^{i}$" for i in range(y_tick_count)]
        y_tick_positions, y_tick_labels = add_minor_log_ticks(y_tick_positions, y_tick_labels, base=log_base, resolution=resolution)
        plt.yticks(y_tick_positions, y_tick_labels)

    # Label the axes
    plt.xlabel("Bisumulation level")
    plt.ylabel("Block size")

    # Plot our data as a heatmap
    plt.imshow(kde, origin="lower")
    plt.savefig(experiment_directory + "results/block_sizes_log-log.svg")
    # plt.show()
