import sys
from math import sqrt, pi, log, floor, e
import numpy as np
from tqdm import tqdm
from matplotlib import pyplot as plt
from matplotlib.colors import LinearSegmentedColormap
from matplotlib.colors import Colormap
from typing import Any, Type, Protocol
from summary_loader.loader_functions import get_fixed_point, get_sizes
from functools import partial

# Using the following setting:
# n1 = parameterized_diagonal_multivariate_gaussian(means, standard_deviations)
# n2 = partial(parametric_diagonal_multivariate_gaussian, means=means, standard_deviations=standard_deviations)
# n3 = parametric_diagonal_multivariate_gaussian_speed
#
# We found that:
# n1_speed > n3_speed = n4_speed

ArgsType = tuple[Any]
KwargsType = dict[str, Any]

# class ParadisoSemiCmap(Colormap):
#     def __init__(self) -> None:
#         super().__init__("paradiso", 256)

#         SKEW = 0.8
#         s = -1/(e*log(2*(1-SKEW), e))
#         self.numerator = -s**2
#         offset_term = sqrt(4*s**2+1)/2
#         self.horizontal_offset = offset_term - 1/2
#         self.vertical_offset = -offset_term - 1/2
    
#     def __call__(self, X: np.ndarray, *args, bytes=False, **kwargs) -> tuple[float,float,float,float]:
#         xa = np.array(X, copy=True)
#         mix_factor=self.numerator/(xa + self.horizontal_offset)-self.vertical_offset
#         mix_factor = np.expand_dims(mix_factor, axis=2)

#         rgba = plt.cm.YlGnBu(xa)
#         rgba[:,:,:3] = mix_factor*rgba[:,:,:3] + (1-mix_factor)*1  # Change the "rgb" channels (and leave the "a" channel intact)
#         if bytes:
#             rgba = np.round(rgba*255).astype(np.uint8)
#         return rgba

# PARADISO: Colormap = ParadisoSemiCmap()
INFERNO: Colormap = plt.cm.inferno
# PARADISO: Colormap = plt.cm.inferno_r

colors = INFERNO(np.arange(INFERNO.N))
# inverted_colors = 1 - colors[:, :3]  # Invert rgb values
# PARADISO: Colormap = LinearSegmentedColormap.from_list("paradiso", inverted_colors)

def whiten_cmap(cmap: Colormap, name: str, skew: float = 0.8) -> Colormap:
    xa = np.arange(cmap.N)/cmap.N
    s = -1/(e*log(2*(1-skew), e))
    numerator = -s**2
    offset_term = sqrt(4*s**2+1)/2
    horizontal_offset = offset_term - 1/2
    vertical_offset = -offset_term - 1/2
    mix_factor=numerator/(xa + horizontal_offset)-vertical_offset
    mix_factor = np.expand_dims(mix_factor, axis=1)

    rgba = cmap(xa)
    rgba[:,:3] = mix_factor*rgba[:,:3] + (1-mix_factor)*1  # Change rgb values
    return LinearSegmentedColormap.from_list(name, rgba)

SKEW = 0.8
PARADISO: Colormap = whiten_cmap(plt.cm.YlGnBu, "paradiso", SKEW)

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


# def add_minor_log_ticks(
#     positions: np.ndarray,
#     labels: list[str],
#     base: int = 10,
#     resolution: int | None = None,
# ) -> tuple[np.ndarray, list[str]]:
#     new_positions = []
#     new_labels = []
#     major_tick_difference = positions[1] - positions[0]
#     # minor_tick_offsets = [base**(i/base)*(major_tick_difference/base) for i in range(1, base)]
#     minor_tick_offsets = [log(i, base) * major_tick_difference for i in range(2, base)]
#     minor_tick_labels = ["" for i in range(base - 2)]
#     for i in range(len(positions) - 1):
#         # Add a major tick
#         new_positions.append(positions[i])
#         # Add the minor ticks
#         new_positions += [positions[i] + offset for offset in minor_tick_offsets]
#         # Add a major tick label
#         new_labels.append(labels[i])
#         # Add empty labels for the minor ticks
#         new_labels += minor_tick_labels
#     # Add the last major tick and label
#     new_positions.append(positions[-1])
#     new_labels.append(labels[-1])
#     # Add the remaining minor ticks (this is important if the plot does not end with a major tick)
#     if resolution is not None:
#         for offset in minor_tick_offsets:
#             minor_tick_position = positions[-1] + offset
#             if minor_tick_position > resolution:
#                 break
#             new_positions.append(minor_tick_position)
#             new_labels.append("")
#     return np.asarray(new_positions), new_labels

def add_minor_log_ticks(
    positions: np.ndarray,
    labels: list[str],
    base: int = 10,
    start: float|None = None,
    end: float|None = None,
) -> tuple[np.ndarray, list[str]]:
    if not positions.ndim == 1:
        raise ValueError(f"The `positions` parameter is supposed to have 1 dimension, but instead it has {positions.ndim}")
    
    major_tick_difference = positions[1] - positions[0]
    if not np.all(np.isclose(positions[1:]-positions[:-1], major_tick_difference)):
        raise ValueError("The `positions` parameter did not have evenly-spaced values")

    new_positions = []
    new_labels = []
    minor_tick_offsets = [log(i, base) * major_tick_difference for i in range(2, base)]
    minor_tick_label = ""
    minor_tick_labels = [minor_tick_label for i in range(base - 2)]

    # Add the (potential) preceding minor ticks
    for offset in reversed(minor_tick_offsets):
        minor_tick_position = positions[0]-major_tick_difference+offset
        if minor_tick_position < start:
            break
        new_positions.append(minor_tick_position)
        new_labels.append(minor_tick_label)
    new_positions.reverse()

    # Add the major ticks and the minor ticks between them
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

    # Add the (potential) remaining minor ticks
    for offset in minor_tick_offsets:
        minor_tick_position = positions[-1]+offset
        if minor_tick_position > end:
            break
        new_positions.append(minor_tick_position)
        new_labels.append(minor_tick_label)

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


def kde_via_sampling(
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


class KernelCDFClass(Protocol):
    def __init__(self, level: int, size: int, *args, **kwargs) -> None: ...

    def __call__(self, x: np.ndarray) -> np.ndarray: ...


class EpanechnikovCDF:
    def __init__(self, level: int, size: int, scale: int, epsilon: int) -> None:
        self.size = size
        self.scale = scale
        self.level = level
        self.epsilon = epsilon

        self.term = 1 / 2
        self.factor1 = 3 / (4 * scale)
        self.factor2 = 1 / (4 * scale**3)

    def __apply_epanechnikov_cdf(self, x: np.ndarray) -> np.ndarray:
        y = np.empty_like(x)
        x_centered = x - self.size
        lower_mask = x_centered <= -self.scale
        upper_mask = x_centered >= self.scale
        inner_mask = np.logical_not(
            np.logical_or(lower_mask, upper_mask)
        )  # The complement of the union of the lower and upper masks
        y[lower_mask] = np.zeros_like(x[lower_mask])
        y[upper_mask] = np.ones_like(x[upper_mask])
        y[inner_mask] = (
            self.term
            + self.factor1 * x_centered[inner_mask]
            - self.factor2 * x_centered[inner_mask] ** 3
        )
        return y

    def __call__(self, x: np.ndarray) -> np.ndarray:
        measure = np.zeros((x.shape[0], 1))
        mask = np.logical_and(
            x[:, 0] > self.level - self.epsilon, x[:, 0] < self.level + self.epsilon
        )  # Mask everything outside our uniform distribution
        measure[np.expand_dims(mask, 1)] = self.__apply_epanechnikov_cdf(
            x[:, 1][mask]
        ) / (
            2 * self.epsilon
        )  # The calculation for the Epanechnikov cdf kernel, reweighed by the width of the uniform distribution
        return measure.squeeze()


class UniformCDF:
    def __init__(self, level: int, size: int, scale: int, epsilon: int) -> None:
        self.size = size
        self.scale = scale
        self.level = level
        self.epsilon = epsilon

        self.slope = 1/(2*scale)

    def __apply_uniform_cdf(self, x: np.ndarray) -> np.ndarray:
        y = np.empty_like(x)
        x_centered = x - self.size
        lower_mask = x_centered <= -self.scale
        upper_mask = x_centered >= self.scale
        inner_mask = np.logical_not(
            np.logical_or(lower_mask, upper_mask)
        )  # The complement of the union of the lower and upper masks
        y[lower_mask] = np.zeros_like(x[lower_mask])
        y[upper_mask] = np.ones_like(x[upper_mask])
        y[inner_mask] = self.slope * (x_centered[inner_mask]+self.scale)
        return y

    def __call__(self, x: np.ndarray) -> np.ndarray:
        measure = np.zeros((x.shape[0], 1))
        mask = np.logical_and(
            x[:, 0] > self.level - self.epsilon, x[:, 0] < self.level + self.epsilon
        )  # Mask everything outside our uniform distribution
        measure[np.expand_dims(mask, 1)] = self.__apply_uniform_cdf(
            x[:, 1][mask]
        ) / (
            2 * self.epsilon
        )  # The calculation for the Epanechnikov cdf kernel, reweighed by the width of the uniform distribution
        return measure.squeeze()


def kde_via_sampling(
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


def kde_via_integration(
    kernels: list[KernelCDFClass],
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
    running_mean = np.zeros((resolution, resolution), dtype=np.float64)
    for i in tqdm(range(data_point_count), desc="Calculating average of kernels"):
        image = kernels[i](coordinates).reshape(resolution, resolution + 1).T
        image = image[1:] - image[:-1]  # Definite integration
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
    return running_mean


def generic_universal_kde_via_integral_plot(
    data_points: np.ndarray,
    experiment_directory: str,
    kernel_cdf_generator: Type[KernelCDFClass],
    generator_args: ArgsType = (),
    generator_kwargs: KwargsType = {},
    fixed_point: int | None = None,
    resolution: int = 512,
    weight_type: str = "vertex_based",
    log_size: bool = True,
    log_base: float = 10,
    log_heatmap = True,
    clip: float = 0.0,
    clip_removes=False,
    **kwargs,
) -> None:
    SIZE_PADDING = 0.2
    ACCEPTABLE_WEIGHT_TYPES = {"block_based", "vertex_based"}

    # >>> Check for insane inputs >>>
    if fixed_point is not None and fixed_point < 0:
        raise ValueError(
            f"The fixed_point parameter (set to {fixed_point}) should be set to at least 0 (or None)"
        )

    if resolution < 1:
        raise ValueError(
            f"The resolution parameter (set to {resolution}) should be at least 1"
        )

    if weight_type not in ACCEPTABLE_WEIGHT_TYPES:
        raise ValueError(
            f'The weight_type parameter (set to {weight_type}) should be set to one of: {", ".join(map(lambda x: f'"{x}"', ACCEPTABLE_WEIGHT_TYPES))}'
        )

    if log_base <= 1:
        raise ValueError(
            f"The log_base parameter (set to {log_base}) should be greater than 1"
        )

    if not 0 <= clip <= 1:
        raise ValueError(
            f"The clip parameter (set to {clip}) should be between 0.0 and 1.0"
        )
    # <<< Check for insane inputs <<<

    if fixed_point is not None:
        fixed_point = np.max(data_points[:, 0])

    # Process the data (e.g. normalize the means)
    maximum_size = int(data_points[:, 1].max())
    means, kernel_weights = means_weights_from_data(
        data_points, maximum_size, fixed_point
    )

    data_point_count = data_points.shape[0]

    kernel_CDFs = []
    for i in tqdm(range(data_point_count), desc="Setting up kernels............"):
        kernel_CDFs.append(
            kernel_cdf_generator(
                means[i][0], means[i][1], *generator_args, **generator_kwargs
            )
        )

    levels = np.arange(resolution) / (resolution - 1)
    levels = levels * ((fixed_point + 1) / fixed_point) - 1 / (
        2 * fixed_point
    )  # Adust the range, such that the first and last levels are displayed in full (not half)

    # We will require `resolution+1` coordinates, since we will be using a definite integral to get the final `resolution` pixel values
    if not log_size:
        sizes = np.arange(resolution + 1) / (resolution)
    else:
        # adjusted_start = log_base**(-SIZE_PADDING)
        # adjusted_end = maximum_size * log_base**SIZE_PADDING
        # adjusted_range = adjusted_end-adjusted_start

        sizes = np.logspace(
            -SIZE_PADDING,
            log(maximum_size, log_base) + SIZE_PADDING,
            num=resolution + 1,
            endpoint=True,
            base=log_base,
        ) / (
            maximum_size
        )  # Logarithmically scaled numbers from 0 to 1

    coordinates = np.array(np.meshgrid(levels, sizes)).T.reshape(-1, 2)

    kde = kde_via_integration(
        kernel_CDFs, data_points, kernel_weights, coordinates, resolution, weight_type
    )
    
    if log_heatmap:
        LOG_OFFSET = 0.1
        kde = np.log10(kde * maximum_size + LOG_OFFSET)
        kde -= np.min(kde)
    kde /= np.max(kde)  # Normalize

    if clip > 0.0:
        kde[kde > 1 - clip] = 0 if clip_removes else 1 - clip  # Clip "large" values
        kde /= np.max(
            kde
        )  # Renomalize (note that if clip_removes == True, then np.max(kde) == 1-clip)

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

    if not log_size:
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
        log_range = log(maximum_size, log_base) + 2*SIZE_PADDING
        log_offset = SIZE_PADDING/log_range
        log_scale = log(maximum_size, log_base)/log_range

        y_tick_count = int(floor(log(maximum_size, log_base) + SIZE_PADDING+1))
        y_tick_positions = (
            np.asarray([i for i in range(y_tick_count)], dtype=np.float64)
            / log(maximum_size, log_base)
        )

        y_tick_positions *= log_scale
        y_tick_positions += log_offset
        y_tick_positions *= resolution

        y_tick_labels = [f"${log_base}^{i}$" for i in range(y_tick_count)]
        y_tick_positions, y_tick_labels = add_minor_log_ticks(
            y_tick_positions, y_tick_labels, base=log_base, start=0, end=resolution-1
        )

        plt.yticks(y_tick_positions, y_tick_labels)

    # Label the axes
    plt.xlabel("Bisimulation level")
    plt.ylabel("Block size")

    # Plot our data as a heatmap
    plt.imshow(kde, origin="lower", cmap=PARADISO, interpolation="nearest")
    plt.savefig(experiment_directory + "results/block_sizes_integral_kde.svg", dpi=resolution)


def generic_universal_kde_plot(
    data_points: np.ndarray,
    experiment_directory: str,
    fixed_point: int | None = None,
    resolution: int = 512,
    gaussian_kernels: bool = False,
    variance: float = 0.01,
    scale: float = 0.75,
    weight_type: str = "vertex_based",
    linear_sizes: bool = False,
    log_base: int = 10,
    epsilon: float = 0.5,
    padding: float = 0.05,
    clip: float = 0.95,
    clip_removes=False,
    **kwargs,
) -> None:
    SIZE_PADDING = 0.2

    if fixed_point is None:
        fixed_point = np.max(data_points[:, 0])
    # Process the data (e.g. normalize the means)
    maximum_size = int(data_points[:, 1].max())
    means, kernel_weights = means_weights_from_data(
        data_points, maximum_size, fixed_point
    )

    scale /= maximum_size
    epsilon = (
        (1 - padding) * epsilon / fixed_point
    )  # Using (1 - padding) allows for the regions of adjacent levels not to overlap

    data_point_count = data_points.shape[0]

    if gaussian_kernels:
        standard_deviation = sqrt(variance)
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
    )  # Adust the range, such that the first and last levels are displayed in full (not half)

    if linear_sizes:
        sizes = np.arange(resolution) / (resolution - 1)
    else:
        sizes = np.logspace(
            0 - SIZE_PADDING,
            log(maximum_size, log_base) + SIZE_PADDING,
            num=resolution,
            endpoint=True,
            base=log_base,
        ) / (
            log_base ** (log(maximum_size, log_base) + SIZE_PADDING)
            - log_base ** (0 - SIZE_PADDING)
        )  # Logarithmically scaled numbers from 0 to 1

    coordinates = np.array(np.meshgrid(levels, sizes)).T.reshape(-1, 2)

    kde = kde_via_sampling(
        kernels, data_points, kernel_weights, coordinates, resolution, weight_type
    )

    kde /= np.max(kde)  # Normalize
    if not linear_sizes:
        kde[kde > 1 - clip] = 0 if clip_removes else 1 - clip  # Clip "large" values
        kde /= np.max(
            kde
        )  # Renomalize (note that if clip_removes == True, then np.max(kde) == 1-clip)

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
        start = 1 - log_base ** (-SIZE_PADDING)
        log_maximum_size = log(maximum_size, log_base)
        log_maximum_size_padded = log(maximum_size + SIZE_PADDING, log_base)
        y_tick_count = int(floor(log_maximum_size_padded)) + 1
        y_tick_positions = (
            np.asarray([i for i in range(y_tick_count)], dtype=np.float64)
            / y_tick_count
        )
        y_tick_positions = (y_tick_positions * (log_maximum_size - start) + start) * (
            1 / log_maximum_size_padded
        )
        y_tick_positions *= resolution
        y_tick_labels = [f"${log_base}^{i}$" for i in range(y_tick_count)]
        y_tick_positions, y_tick_labels = add_minor_log_ticks(
            y_tick_positions, y_tick_labels, base=log_base, start=0, end=resolution-1
        )
        plt.yticks(y_tick_positions, y_tick_labels)

    # Label the axes
    plt.xlabel("Bisimulation level")
    plt.ylabel("Block size")

    # Plot our data as a heatmap
    plt.imshow(kde, origin="lower", cmap=PARADISO, interpolation="nearest")
    plt.savefig(experiment_directory + "results/block_sizes_generic_universal_kde.svg", dpi=resolution)


def gaussian_log_log_kde_plot(
    data_points: np.ndarray,
    experiment_directory: str,
    fixed_point: int | None = None,
    dimension: int = 2,
    resolution: int = 512,
    variance_type: str = "scott",
    variance_factor: float = 0.01,
    weight_type: str = "vertex_based",
    log_base: int = 10,
    **kwargs,
) -> None:
    if fixed_point is None:
        fixed_point = np.max(data_points[:, 0])

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
    kde = kde_via_sampling(
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
            y_tick_positions, y_tick_labels, base=log_base, start=0, end=resolution-1
        )
        plt.yticks(y_tick_positions, y_tick_labels)

    # Label the axes
    plt.xlabel("Bisimulation level")
    plt.ylabel("Block size")

    # Plot our data as a heatmap
    plt.imshow(kde, origin="lower", cmap=PARADISO, interpolation="nearest")
    plt.savefig(experiment_directory + "results/block_sizes_gaussian_log_log_kde.svg", dpi=resolution)


if __name__ == "__main__":
    experiment_directory = sys.argv[1]

    # TODO Add singletons
    # TODO Allow to save figs in exact locations, with different names
    # TODO Add titles to the figures

    # Load in the data
    fixed_point = get_fixed_point(experiment_directory)
    block_sizes = get_sizes(experiment_directory, fixed_point)
    data_points = []
    for level, data in enumerate(block_sizes):
        for size, count in data["Block sizes"].items():
            data_points.append((level, size, count))
    data_points = np.stack(data_points)  # shape = number_of_data_points x 3

    # Create a multivariate Gaussian kde plot, shown in log-log scale
    gaussian_log_log_kwargs = {
        "dimension": 2,
        "resolution": int(512 * 2**0),
        "variance_type": "scott",
        "variance_factor": 0.001,
        "weight_type": "vertex_based",
        "log_base": 10,
    }
    gaussian_log_log_kde_plot(data_points, experiment_directory, fixed_point, **gaussian_log_log_kwargs)
    # plt.show()

    # Create a 2D (Gaussian or Epanechnikov)-universal kde plot, shown in lin-log scale
    generic_universal_kwargs = {
        "resolution": 512,
        "gaussian_kernels": False,
        "variance": 0.001,
        "scale": 0.5,
        "weight_type": "vertex_based",
        "linear_sizes": False,
        "log_base": 10,
        "epsilon": 0.5,
        "padding": 0.05,
        "clip": 0.90,
        "clip_removes": False,
    }
    generic_universal_kde_plot(data_points, experiment_directory, fixed_point, **generic_universal_kwargs)
    # plt.show()

    # Create a 2D (Gaussian or Epanechnikov or custom)-universal kde plot that uses integration instead of sampling to get the heatmap values, shown in lin-log scale
    via_integration_kwargs = {
        "dimension": 2,
        "resolution": 512,
        "weight_type": "vertex_based",
        "log_size": True,
        "log_base": 10,
        "log_heatmap": True,
        "clip": 0.00,
        "clip_removes": False,
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
    
    generic_universal_kde_via_integral_plot(
        data_points,
        experiment_directory,
        UniformCDF,
        epanechnikov_args,
        epanechnikov_kwargs,
        fixed_point,
        **via_integration_kwargs,
    )
    # plt.show()
