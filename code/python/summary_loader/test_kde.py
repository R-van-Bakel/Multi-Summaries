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
def parametric_diagonal_multivariate_gaussian(
    x: np.ndarray, means: np.ndarray, standard_deviations: np.ndarray
) -> np.ndarray:
    dimension = means.size
    variances = np.square(standard_deviations)
    determinant = np.prod(variances)

    normalization_constant = (2 * pi) ** (-dimension / 2) * determinant ** (-1 / 2)
    measure = np.exp((-1 / 2) * np.square(x - means) * np.reciprocal(variances))
    return normalization_constant * measure


# Create a callable parameterized 2D kernel that is uniform in one direction and Epanechnikov in the other
class parameterized_uniform_epanechnikov:
    def __init__(self, level: int, size: int, scale: int, epsilon: int = 0.1) -> None:
        self.level = level
        self.epsilon = epsilon
        self.size = size
        self.scale = scale
        self.normalization_constant = scale * 3 / 4

    def __call__(self, x: np.ndarray) -> np.ndarray:
        measure = np.zeros((x.shape[0], 1))
        mask = np.logical_and(
            x[:, 0] > self.level - self.epsilon, x[:, 0] < self.level + self.epsilon
        )  # Mask everything outside our uniform distribution
        measure[np.expand_dims(mask, 1)] = np.maximum(
            0, 1 - ((x[:, 1][mask] - self.size) / self.scale) ** 2
        ) / (
            2 * self.epsilon
        )  # The calculation for the Epanechnikov kernel, reweighed by the width of the uniform distribution
        return self.normalization_constant * measure.squeeze()


# Create a callable parameterized 2D kernel that is uniform in one direction and Gaussian in the other
class parameterized_uniform_gaussian:
    def __init__(
        self, level: int, size: int, standard_deviation: int, epsilon: int = 0.1
    ) -> None:
        self.level = level
        self.epsilon = epsilon
        self.size = size
        determinant = variance = standard_deviation**2

        self.normalization_constant = (2 * pi * determinant) ** (-1 / 2)
        self.partial_exponent = -1 / (2 * variance)

    def __call__(self, x: np.ndarray) -> np.ndarray:
        measure = np.zeros((x.shape[0], 1))
        mask = np.logical_and(
            x[:, 0] > self.level - self.epsilon, x[:, 0] < self.level + self.epsilon
        )  # Mask everything outside our uniform distribution
        measure[np.expand_dims(mask, 1)] = np.exp(
            np.square(x[:, 1][mask] - self.size) * self.partial_exponent
        ) / (
            2 * self.epsilon
        )  # The calculation for the Gaussian, reweighed by the width of the uniform distribution
        return self.normalization_constant * measure.squeeze()


# Create a callable parameterized Gaussian. By parameterizing a head of time this function can effectively be reused several times
class parameterized_diagonal_multivariate_gaussian:
    def __init__(self, means: np.ndarray, standard_deviations: np.ndarray) -> None:
        self.means = means
        dimension = means.size
        variances = np.square(standard_deviations)
        determinant = np.prod(variances)

        self.normalization_constant = (2 * pi) ** (-dimension / 2) * determinant ** (
            -1 / 2
        )
        self.partial_exponent = (-1 / 2) * np.reciprocal(variances)

    def __call__(self, x: np.ndarray) -> np.ndarray:
        measure = np.exp(
            np.sum(np.square(x - self.means) * self.partial_exponent, axis=1)
        )
        return self.normalization_constant * measure


def add_minor_log_ticks(
    positions: np.ndarray,
    labels: list[str],
    base: int = 10,
    resolution: int | None = None,
) -> tuple[np.ndarray, list[str]]:
    new_positions = []
    new_labels = []
    major_tick_difference = positions[1] - positions[0]
    # minor_tick_offsets = [base**(i/base)*(major_tick_difference/base) for i in range(1, base)]
    minor_tick_offsets = [log(i, base) * major_tick_difference for i in range(2, base)]
    minor_tick_labels = ["" for i in range(base - 2)]
    for i in range(len(positions) - 1):
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
            minor_tick_position = positions[-1] + offset
            if minor_tick_position > resolution:
                break
            new_positions.append(minor_tick_position)
            new_labels.append("")
    return np.asarray(new_positions), new_labels


def means_weights_from_data(
    data_points: np.ndarray, maximum_size: int, fixed_point: int
) -> tuple[np.ndarray, np.ndarray]:
    # Each row contains 1) a bisimulation level and 2) a block size
    means = data_points[:, :2].astype(np.float64)

    # The highest size in our input
    maximum_size = int(means[:, 1].max())

    # Normalize the columns
    means[:, 0:1] = means[:, 0:1] / fixed_point
    means[:, 1:2] = means[:, 1:2] / maximum_size

    # Each row is one element indicating the count of occurences of the repsective block size
    # e.g. kernel_weights[0] specifies how many block of size means[1] appear at level means[0]
    kernel_weights = data_points[:, 2:3]
    return means, kernel_weights


def determine_standard_deviation(
    data_point_count: int, dimension: int, variance_type: str
) -> np.ndarray:
    # Set the variance according to either Scott's or Silverman's rule
    # Note that 1) the covariance matrix is effectively a diagonal matrix with the same value sqrt(variance) along its diagonal
    # and 2) we use the exact same variance for each kernel (as opposed to the means which are unique for every kernel)
    if variance_type == "scott":
        variance = data_point_count ** (-1.0 / (dimension + 4))
    elif variance_type == "silverman":
        variance = (data_point_count * (dimension + 2) / 4.0) ** (
            -1.0 / (dimension + 4)
        )
    else:
        raise ValueError('`variance_type` should be one of "scott" or "silverman"')
    standard_deviations = np.full(
        shape=dimension, fill_value=sqrt(variance), dtype=np.float64
    )
    return standard_deviations


def kde_from_kernels(
    kernels: (
        list[parameterized_diagonal_multivariate_gaussian]
        | list[parameterized_uniform_gaussian]
    ),
    data_points: np.ndarray,
    kernel_weights: np.ndarray,
    coordinates: np.ndarray,
    resolution: int,
    weight_type: str,
) -> np.ndarray:
    # Calculate the average, weighted kernel
    # The rule `running_mean*(weight/new_weight) + image(current_weight/new_weight)` prevents values from exploding or disappearing
    weight = 0
    data_point_count = len(kernels)
    running_mean = np.zeros((resolution**2), dtype=np.float64)
    for i in tqdm(range(data_point_count), desc="Calculating average of kernels"):
        image = kernels[i](coordinates)
        if weight_type == "block_based":
            current_weight = kernel_weights[i]
        elif weight_type == "vertex_based":
            current_weight = kernel_weights[i] * data_points[i][1]
        else:
            raise ValueError(
                '`weight_type` should be set to one of: "block_based" or "vertex_based"'
            )
        new_weight = weight + current_weight
        running_mean = running_mean * (weight / new_weight) + image * (
            current_weight / new_weight
        )
        weight = new_weight
    return running_mean.reshape(resolution, resolution).T

def generic_universal_kde_plot(
    data_points: np.ndarray,
    fixed_point: int,
    resolution: int = 512,
    log_base: int = 10,
    variance_factor: float = 0.01,
    weight_type: str = "vertex_based",
    epsilon: float = 0.5,
    gaussian_kernels: bool = False,
    linear_sizes: bool = False,
    scale: float = 0.75,
    clip: float = 4,
    padding: float = 0.05,
    **kwargs,) -> None:

    if not linear_sizes:
        scale = scale * 1/(1e+6)
        epsilon = (
            (1 - padding) * epsilon / fixed_point
        )  # Using (1 - padding) allows for the regions of adjacent levels not to overlap

    # Process the data (e.g. normalize the means)
    maximum_size = int(data_points[:, 1].max())
    means, kernel_weights = means_weights_from_data(
        data_points, maximum_size, fixed_point
    )
    # print("means[:, 0] max:", means[:, 0].max())
    # print("means[:, 1] max:", means[:, 1].max())
    # print("means[:, 0] min:", means[:, 0].min())
    # print("means[:, 1] min:", means[:, 1].min())
    # exit(0)

    # We use `variance_factor` if we want to manually scale the variance
    data_point_count = data_points.shape[0]
    standard_deviation = sqrt(variance_factor)

    if gaussian_kernels:
        # Create all the separate Gaussian kernels, using the specified means
        kernels = []
        for i in tqdm(range(data_point_count), desc="Setting up kernels............"):
            kernels.append(
                parameterized_uniform_gaussian(
                    means[i][0], means[i][1], standard_deviation, epsilon=epsilon
                )
            )
    else:
        kernels = []
        for i in tqdm(range(data_point_count), desc="Setting up kernels............"):
            kernels.append(
                parameterized_uniform_epanechnikov(
                    means[i][0], means[i][1], scale=scale, epsilon=epsilon
                )
            )

        levels = np.arange(resolution) / (resolution - 1)
        levels = levels * ((fixed_point + 1) / fixed_point) - 1 / (
            2 * fixed_point
        )  # Adust the range, such that the first and last levels are printed in full (not half)

    size_padding = 0.2

    if linear_sizes:
        sizes = np.arange(resolution) / (resolution - 1)
    else:
        sizes = (
            np.logspace(
                0-size_padding,
                log(maximum_size, log_base)+size_padding,
                num=resolution,
                endpoint=True,
                base=log_base,
            )
            / (log_base**(log(maximum_size, log_base)+size_padding)-log_base**(0-size_padding))
        )  # Logarithmically scaled numbers from 0 to 1

    coordinates = np.array(np.meshgrid(levels, sizes)).T.reshape(-1, 2)

    kde = kde_from_kernels(
        kernels, data_points, kernel_weights, coordinates, resolution, weight_type
    )
    
    if not linear_sizes:
        kde /= np.max(kde)
        kde *= clip
        kde[kde>1] = 1

    x_tick_labels = range(fixed_point + 1)
    x_tick_positions = [
        label * (resolution - 1) / fixed_point for label in x_tick_labels
    ]
    x_tick_positions = [
        position * (fixed_point / (fixed_point + 1))
        + resolution / (2 * (fixed_point + 1))
        for position in x_tick_positions
    ]  # Since we rescaled the levels earlier, we will have to change the ticks accordingly
    plt.xticks(x_tick_positions, x_tick_labels)

    if linear_sizes:
        y_tick_count = 6
        y_tick_labels = [
            int(round(i * maximum_size / (y_tick_count - 1)))
            for i in range(y_tick_count)
        ]
        y_tick_positions = [
            label * (resolution - 1) / maximum_size for label in y_tick_labels
        ]
        plt.yticks(y_tick_positions, y_tick_labels)
    else:
        start = 1-log_base**(-size_padding)
        log_maximum_size = log(maximum_size, log_base)
        log_maximum_size_padded = log(maximum_size+size_padding, log_base)
        y_tick_count = int(floor(log_maximum_size_padded)) + 1
        y_tick_positions = np.asarray(
            [i for i in range(y_tick_count)], dtype=np.float64
        )/y_tick_count
        y_tick_positions = (y_tick_positions * (log_maximum_size-start) + start) * (1/log_maximum_size_padded)
        y_tick_positions *= resolution
        print("factor:", (log_maximum_size-start) * (1/log_maximum_size_padded))
        # y_tick_positions = np.asarray(
        #     [i for i in range(y_tick_count)], dtype=np.float64
        # ) * resolution / y_tick_count
        y_tick_labels = [f"${log_base}^{i}$" for i in range(y_tick_count)]
        y_tick_positions, y_tick_labels = add_minor_log_ticks(
            y_tick_positions, y_tick_labels, base=log_base, resolution=resolution
        )
        plt.yticks(y_tick_positions, y_tick_labels)

    # Label the axes
    plt.xlabel("Bisumulation level")
    plt.ylabel("Block size")

    # Plot our data as a heatmap
    plt.imshow(kde, origin="lower", interpolation="nearest")
    # plt.savefig(experiment_directory + "results/block_sizes_log-log_kde.svg")
    plt.show()


def gaussian_log_log_kde_plot(
    data_points: np.ndarray,
    fixed_point: int,
    experiment_directory: str,
    dimension: int = 2,
    resolution: int = 512,
    log_base: int = 10,
    variance_type: str = "scott",
    variance_factor: float = 0.01,
    weight_type: str = "vertex_based",
    **kwargs,
) -> None:

    # Process the data (e.g. normalize the means)
    maximum_size = int(data_points[:, 1].max())
    means, kernel_weights = means_weights_from_data(
        data_points, maximum_size, fixed_point
    )

    # We use `variance_factor` if we want to manually scale the variance
    data_point_count = data_points.shape[0]
    standard_deviations = determine_standard_deviation(
        data_point_count, dimension, variance_type
    ) * sqrt(variance_factor)

    # Create all the separate Gaussian kernels, using the specified means
    kernels = []
    for i in tqdm(range(data_point_count), desc="Setting up kernels............"):
        kernels.append(
            parameterized_diagonal_multivariate_gaussian(means[i], standard_deviations)
        )

    # Create the coordinates, used for sampling the kde kernels
    log_sizes = (
        np.logspace(
            0, log(maximum_size, log_base), num=resolution, endpoint=True, base=log_base
        )
        / maximum_size
    )  # Logarithmically scaled numbers from 0 to 1
    log_levels = (
        np.logspace(
            0,
            log(fixed_point + 1, log_base),
            num=resolution,
            endpoint=True,
            base=log_base,
        )
        - 1
    ) / fixed_point  # Logarithmically scaled numbers from 0 to 1
    log_coordinates = np.array(np.meshgrid(log_levels, log_sizes)).T.reshape(-1, 2)

    # Get the final result from sampling our kernels at the given coordinates
    kde = kde_from_kernels(
        kernels, data_points, kernel_weights, log_coordinates, resolution, weight_type
    )

    # Set the x and y ticks differently for small numbers (less that log_base) than for big numbers
    if fixed_point < log_base:
        x_tick_positions = np.asarray(
            [log(i, log_base) for i in range(1, fixed_point + 2)]
        )
        x_tick_positions = x_tick_positions * (
            (resolution - 1) / float(np.max(x_tick_positions))
        )
        x_tick_labels = [f"{i}" for i in range(fixed_point + 1)]
        plt.xticks(x_tick_positions, x_tick_labels)
    else:
        log_fixed_point = log(fixed_point + 1, log_base)
        x_tick_count = int(floor(log_fixed_point)) + 1
        x_tick_positions = np.asarray(
            [i for i in range(x_tick_count)], dtype=np.float64
        ) * (resolution / log_fixed_point)
        x_tick_labels = [f"${log_base}^{i}$" for i in range(x_tick_count)]
        x_tick_positions, x_tick_labels = add_minor_log_ticks(
            x_tick_positions, x_tick_labels, base=log_base, resolution=resolution
        )
        plt.xticks(x_tick_positions, x_tick_labels)

    if maximum_size < log_base:
        y_tick_positions = np.asarray(
            [log(i, log_base) for i in range(1, maximum_size + 2)]
        )
        y_tick_positions = y_tick_positions * (
            (resolution - 1) / float(np.max(y_tick_positions))
        )
        y_tick_labels = [f"{i}" for i in range(maximum_size + 1)]
        plt.yticks(y_tick_positions, y_tick_labels)
    else:
        log_maximum_size = log(maximum_size, log_base)
        y_tick_count = int(floor(log_maximum_size)) + 1
        y_tick_positions = np.asarray(
            [i for i in range(y_tick_count)], dtype=np.float64
        ) * (resolution / log_maximum_size)
        y_tick_labels = [f"${log_base}^{i}$" for i in range(y_tick_count)]
        y_tick_positions, y_tick_labels = add_minor_log_ticks(
            y_tick_positions, y_tick_labels, base=log_base, resolution=resolution
        )
        plt.yticks(y_tick_positions, y_tick_labels)

    # Label the axes
    plt.xlabel("Bisumulation level")
    plt.ylabel("Block size")

    # Plot our data as a heatmap
    plt.imshow(kde, origin="lower")
    plt.savefig(experiment_directory + "results/block_sizes_log-log_kde.svg")


if __name__ == "__main__":
    experiment_directory = sys.argv[1]

    # Set the parameters
    # TODO allow these to not be hard-coded
    kwargs = {
        "experiment_directory": experiment_directory,
        "dimension": 2,
        "resolution": 512*2,
        "log_base": 10,
        "variance_type": "scott",
        "variance_factor": 0.01,
        "weight_type": "vertex_based",
        "epsilon": 0.5,
        "gaussian_kernels": False,
        "linear_sizes": False,
        "scale": 0.75,
        "clip": 16.0,
        "padding": 0.05,
    }

    # Load in the data
    fixed_point = get_fixed_point(experiment_directory)
    block_sizes = get_sizes(experiment_directory, fixed_point)
    data_points = []
    for level, data in enumerate(block_sizes):
        for size, count in data["Block sizes"].items():
            data_points.append((level, size, count))
    
    data_points = np.stack(data_points)  # shape = number_of_data_points x 3

    # # Create a multivariate Gaussian kde plot, shown in log-log scale
    # gaussian_log_log_kde_plot(data_points, fixed_point, **kwargs)

    # Create a 2D (Gaussian or Epanechnikov)-universal kde plot, shown in lin-log scale
    generic_universal_kde_plot(data_points, fixed_point, **kwargs)  # TODO name this thing

    # (
    #     dimension,
    #     resolution,
    #     log_base,
    #     variance_type,
    #     variance_factor,
    #     weight_type,
    #     epsilon,
    # ) = map(
    #     kwargs.get,
    #     [
    #         "dimension",
    #         "resolution",
    #         "log_base",
    #         "variance_type",
    #         "variance_factor",
    #         "weight_type",
    #         "epsilon",
    #     ],
    # )

    # levels = np.arange(resolution)*(fixed_point/(resolution-1))
    # sizes = np.arange(resolution)*(12/resolution)
    # coordinates = np.array(np.meshgrid(levels, sizes)).T.reshape(-1, 2)

    # result = np.zeros((coordinates.shape[0], 1))
    # # print(coordinates)
    # epsilon = 0.1
    # mean = (2, 10)
    # level = mean[0]
    # size = mean[1]
    # mask = np.logical_and(coordinates[:, 0]>level-epsilon,coordinates[:, 0]<level+epsilon)
    # valid_coords = coordinates[mask]
    # coordinates[~mask] *= 0
    # # print(result[mask].shape)
    # # print(coordinates[:, 0][mask].shape)
    # # print(mask.shape)
    # result[np.expand_dims(mask,1)] = coordinates[:, 0][mask]
    # # print(result)

    # print(np.unique(coordinates[:,1]))

    # # Create the coordinates, used for sampling the kde kernels
    # log_sizes = (
    #     np.logspace(
    #         0, log(maximum_size, log_base), num=resolution, endpoint=True, base=log_base
    #     )
    #     / maximum_size
    # )  # Logarithmically scaled numbers from 0 to 1
    # log_levels = (
    #     np.logspace(
    #         0,
    #         log(fixed_point + 1, log_base),
    #         num=resolution,
    #         endpoint=True,
    #         base=log_base,
    #     )
    #     - 1
    # ) / fixed_point  # Logarithmically scaled numbers from 0 to 1
    # log_coordinates = np.array(np.meshgrid(log_levels, log_sizes)).T.reshape(-1, 2)

    # u_n_1 = parameterized_uniform_gaussian(level=0, size=8, standard_deviation=2, epsilon=0.5)
    # u_n_2 = parameterized_uniform_gaussian(level=1, size=2, standard_deviation=2, epsilon=0.5)
    # u_n_3 = parameterized_uniform_gaussian(level=2, size=10, standard_deviation=2, epsilon=0.5)
    # u_n_4 = parameterized_uniform_gaussian(level=3, size=4, standard_deviation=2, epsilon=0.5)
    # u_n = lambda x: (u_n_1(x) + u_n_2(x) + u_n_3(x) + u_n_4(x))/4
    # out = u_n(coordinates).reshape(resolution,resolution).T

    # # Create all the separate Gaussian kernels, using the specified means
    # kernels = []
    # for i in tqdm(range(data_point_count), desc="Setting up kernels............"):
    #     kernels.append(
    #         parameterized_diagonal_multivariate_gaussian(means[i], standard_deviations)
    #     )

    # log_sizes = (
    #     np.logspace(
    #         0, log(maximum_size, log_base), num=resolution, endpoint=True, base=log_base
    #     )
    #     / maximum_size
    # )  # Logarithmically scaled numbers from 0 to 1
    # log_levels = (
    #     np.logspace(
    #         0,
    #         log(fixed_point + 1, log_base),
    #         num=resolution,
    #         endpoint=True,
    #         base=log_base,
    #     )
    #     - 1
    # ) / fixed_point  # Logarithmically scaled numbers from 0 to 1
    # log_coordinates = np.array(np.meshgrid(log_levels, log_sizes)).T.reshape(-1, 2)

    # # Calculate the average, weighted kernel
    # # The rule `running_mean*(weight/new_weight) + image(current_weight/new_weight)` prevents values from exploding or disappearing
    # weight = 0
    # running_mean = np.zeros((resolution**2), dtype=np.float64)
    # for i in tqdm(range(data_point_count), desc="Calculating average of kernels"):
    #     image = kernels[i](log_coordinates)
    #     if weight_type == "block_based":
    #         current_weight = kernel_weights[i]
    #     elif weight_type == "vertex_based":
    #         current_weight = kernel_weights[i] * data_points[i][1]
    #     else:
    #         raise ValueError(
    #             '`weight_type` should be set to one of: "block_based" or "vertex_based"'
    #         )
    #     new_weight = weight + current_weight
    #     running_mean = running_mean * (weight / new_weight) + image * (
    #         current_weight / new_weight
    #     )
    #     weight = new_weight
    # kde = running_mean.reshape(resolution, resolution).T
