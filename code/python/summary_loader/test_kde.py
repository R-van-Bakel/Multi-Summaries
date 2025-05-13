import sys
from math import sqrt, pi, log, floor, e
import numpy as np
from tqdm import tqdm
from matplotlib import pyplot as plt
from matplotlib.colors import LinearSegmentedColormap, Colormap, LogNorm, SymLogNorm
from matplotlib.ticker import LogFormatterSciNotation
from typing import Any, Type, Protocol
from summary_loader.loader_functions import (
    get_fixed_point,
    get_sizes_and_split_blocks,
    get_statistics,
)
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

INFERNO: Colormap = plt.cm.inferno

colors = INFERNO(np.arange(INFERNO.N))
inverted_colors = 1 - colors[:, :3]  # Invert rgb values
NEGATIVE_INFERNO: Colormap = LinearSegmentedColormap.from_list(
    "paradiso", inverted_colors
)


def whiten_cmap(cmap: Colormap, name: str, skew: float = 0.8) -> Colormap:
    """
    This function takes in a color map and returns a whitened version of it.

    Parameters
    ----------
    cmap : Colormap
        A Matplotlib Colormap. It will be used as basis to form a whitened Colormap from.
    name : str
        The name that will be given to the resulting Colormap.
    skew : float, optional
        How much the process favours the Colormap colours over the whitening.
        Values closer to 0.5 mix the colours equally, while values close to 1 only visibly whiten the lower values of the Colormap.
        By default 0.8.

    Returns
    -------
    Colormap
        A whitened Colormap.
    """
    xa = np.arange(cmap.N) / cmap.N
    s = -1 / (e * log(2 * (1 - skew), e))
    numerator = -(s**2)
    offset_term = sqrt(4 * s**2 + 1) / 2
    horizontal_offset = offset_term - 1 / 2
    vertical_offset = -offset_term - 1 / 2
    mix_factor = numerator / (xa + horizontal_offset) - vertical_offset
    mix_factor = np.expand_dims(mix_factor, axis=1)

    rgba = cmap(xa)
    rgba[:, :3] = mix_factor * rgba[:, :3] + (1 - mix_factor) * 1  # Change rgb values
    return LinearSegmentedColormap.from_list(name, rgba)


SKEW = 0.8
PARADISO: Colormap = whiten_cmap(plt.cm.YlGnBu, "paradiso", SKEW)


# # A function for calculating a multivariate Gaussian
# def parametric_diagonal_multivariate_gaussian(
#     x: np.ndarray, means: np.ndarray, standard_deviations: np.ndarray
# ) -> np.ndarray:
#     dimension = means.size
#     variances = np.square(standard_deviations)
#     determinant = np.prod(variances)

#     normalization_constant = (2 * pi) ** (-dimension / 2) * determinant ** (-1 / 2)
#     measure = np.exp((-1 / 2) * np.square(x - means) * np.reciprocal(variances))
#     return normalization_constant * measure


# # Create a callable parameterized 2D kernel that is uniform in one direction and Epanechnikov in the other
# class parameterized_uniform_epanechnikov:
#     def __init__(self, level: int, size: int, scale: int, epsilon: int = 0.1) -> None:
#         self.level = level
#         self.epsilon = epsilon
#         self.size = size
#         self.scale = scale
#         self.normalization_constant = scale * 3 / 4

#     def __call__(self, x: np.ndarray) -> np.ndarray:
#         measure = np.zeros((x.shape[0], 1))
#         mask = np.logical_and(
#             x[:, 0] > self.level - self.epsilon, x[:, 0] < self.level + self.epsilon
#         )  # Mask everything outside our uniform distribution
#         measure[np.expand_dims(mask, 1)] = np.maximum(
#             0, 1 - ((x[:, 1][mask] - self.size) / self.scale) ** 2
#         ) / (
#             2 * self.epsilon
#         )  # The calculation for the Epanechnikov kernel, reweighed by the width of the uniform distribution
#         return self.normalization_constant * measure.squeeze()


# # Create a callable parameterized 2D kernel that is uniform in one direction and Gaussian in the other
# class parameterized_uniform_gaussian:
#     def __init__(
#         self, level: int, size: int, standard_deviation: int, epsilon: int = 0.1
#     ) -> None:
#         self.level = level
#         self.epsilon = epsilon
#         self.size = size
#         determinant = variance = standard_deviation**2

#         self.normalization_constant = (2 * pi * determinant) ** (-1 / 2)
#         self.partial_exponent = -1 / (2 * variance)

#     def __call__(self, x: np.ndarray) -> np.ndarray:
#         measure = np.zeros((x.shape[0], 1))
#         mask = np.logical_and(
#             x[:, 0] > self.level - self.epsilon, x[:, 0] < self.level + self.epsilon
#         )  # Mask everything outside our uniform distribution
#         measure[np.expand_dims(mask, 1)] = np.exp(
#             np.square(x[:, 1][mask] - self.size) * self.partial_exponent
#         ) / (
#             2 * self.epsilon
#         )  # The calculation for the Gaussian, reweighed by the width of the uniform distribution
#         return self.normalization_constant * measure.squeeze()


# # Create a callable parameterized Gaussian. By parameterizing a head of time this function can effectively be reused several times
# class parameterized_diagonal_multivariate_gaussian:
#     def __init__(self, means: np.ndarray, standard_deviations: np.ndarray) -> None:
#         self.means = means
#         dimension = means.size
#         variances = np.square(standard_deviations)
#         determinant = np.prod(variances)

#         self.normalization_constant = (2 * pi) ** (-dimension / 2) * determinant ** (
#             -1 / 2
#         )
#         self.partial_exponent = (-1 / 2) * np.reciprocal(variances)

#     def __call__(self, x: np.ndarray) -> np.ndarray:
#         measure = np.exp(
#             np.sum(np.square(x - self.means) * self.partial_exponent, axis=1)
#         )
#         return self.normalization_constant * measure


def add_minor_log_ticks(
    positions: np.ndarray,
    labels: list[str],
    base: int = 10,
    start: float | None = None,
    end: float | None = None,
) -> tuple[np.ndarray, list[str]]:
    """
    Add minor log-tick positions and (empty) labels to the given major ticks.

    Parameters
    ----------
    positions : np.ndarray
        The (evenly-spaced) positions of major ticks.
    labels : list[str]
        The labels associated with the major ticks.
    base : int, optional
        The base that is being used of the log-scale, by default 10.
    start : float | None, optional
        The smallest position to consider for adding minor ticks.
        Setting this to a value smaller than positions[0] allows for minor ticks to precede the first major tick.
        Leaving it at None equates to setting it to positions[0].
        By default None.
    end : float | None, optional
        The largest position to consider for adding minor ticks.
        Setting this to a value larger than positions[-1] allows for minor ticks to follow the last major tick.
        Leaving it at None equates to setting it to positions[-1].
        By default None

    Returns
    -------
    tuple[np.ndarray, list[str]]
        A tuple containing 1) the positions including minor ticks and 2) the tick labels, including empty labels for the minor ticks.

    Raises
    ------
    ValueError
        The positions array was not one-dimensional.
    ValueError
        The positions were not evenly spaced.
    """
    if not positions.ndim == 1:
        raise ValueError(
            f"The `positions` parameter is supposed to have 1 dimension, but instead it has {positions.ndim}"
        )

    start = start if start is not None else positions[0]
    end = end if end is not None else positions[-1]

    major_tick_difference = positions[1] - positions[0]
    if not np.all(np.isclose(positions[1:] - positions[:-1], major_tick_difference)):
        raise ValueError("The `positions` parameter did not have evenly-spaced values")

    new_positions = []
    new_labels = []
    minor_tick_offsets = [log(i, base) * major_tick_difference for i in range(2, base)]
    minor_tick_label = ""
    minor_tick_labels = [minor_tick_label for i in range(base - 2)]

    # Add the (potential) preceding minor ticks
    for offset in reversed(minor_tick_offsets):
        minor_tick_position = positions[0] - major_tick_difference + offset
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
        minor_tick_position = positions[-1] + offset
        if minor_tick_position > end:
            break
        new_positions.append(minor_tick_position)
        new_labels.append(minor_tick_label)

    new_ticks = (np.asarray(new_positions), new_labels)
    return new_ticks


def means_weights_from_data(data_points: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """
    Extract the means and kernel weights from the data points.

    Parameters
    ----------
    data_points : np.ndarray
        An (n x 3) array containing means [:, :2] and kernel weights [:, 2:3]

    Returns
    -------
    tuple[np.ndarray, np.ndarray]
        A tuple containing the normalized means and the (non-normalized) kernel weights
    """
    # Each row contains 1) a bisimulation level and 2) a block size
    means = data_points[:, :2].astype(np.float64)

    # The highest level in our input
    maximum_level = int(
        means[:, 0].max()
    )  # This may or may not be the fixed point, depending whether the summarization was stopped stopped early

    # The highest size in our input
    maximum_size = int(means[:, 1].max())

    # Normalize the columns
    means[:, 0:1] = means[:, 0:1] / maximum_level
    means[:, 1:2] = means[:, 1:2] / maximum_size

    # Each row is one element indicating the count of occurences of the repsective block size
    # e.g. kernel_weights[0] specifies how many block of size means[1] appear at level means[0]
    kernel_weights = data_points[:, 2:3]
    return means, kernel_weights


# def determine_standard_deviation(
#     data_point_count: int, dimension: int, variance_type: str
# ) -> np.ndarray:
#     """
#     Set the standard deviation of the multivariate Gaussian kernel using either Scott's or Silverman' rule.

#     Parameters
#     ----------
#     data_point_count : int
#         The data point count.
#     dimension : int
#         The dimension of the multivariate Gaussian kernel.
#     variance_type : str
#         The variance rule to be applied. It should be one of variance_type should be one of "scott" or "silverman".

#     Returns
#     -------
#     np.ndarray
#         A one-dimensional array containing variances per dimension.
#         Note that the variance will be the same for each dimension.

#     Raises
#     ------
#     ValueError
#         `variance_type` should be one of "scott" or "silverman"
#     """
#     # Set the variance according to either Scott's or Silverman's rule
#     # Note that 1) the covariance matrix is effectively a diagonal matrix with the same value sqrt(variance) along its diagonal
#     # and 2) we use the exact same variance for each kernel (as opposed to the means which are unique for every kernel)
#     if variance_type == "scott":
#         variance = data_point_count ** (-1.0 / (dimension + 4))
#     elif variance_type == "silverman":
#         variance = (data_point_count * (dimension + 2) / 4.0) ** (
#             -1.0 / (dimension + 4)
#         )
#     else:
#         raise ValueError('`variance_type` should be one of "scott" or "silverman"')
#     standard_deviations = np.full(
#         shape=dimension, fill_value=sqrt(variance), dtype=np.float64
#     )
#     return standard_deviations


class KernelCDFClass(Protocol):
    """
    A protocol type used for type hints.
    """

    def __init__(self, level: int, size: int, *args, **kwargs) -> None:
        """
        Initialize the kernels. Note that `level` and `size` are required, but more optional arguments are possible.

        Parameters
        ----------
        level : int
            The level used to set the horizontal component of the mean.
        size : int
            The size used to set the vertical component of the mean.
        """
        ...

    def __call__(self, x: np.ndarray) -> np.ndarray:
        """
        Evaluate the kernel at the coordinates in `x`.

        Parameters
        ----------
        x : np.ndarray
            The coordinates to evaluate the kernel at.

        Returns
        -------
        np.ndarray
            The result of mapping `x` through the kernel.
        """
        ...


class EpanechnikovCDF:
    """
    A class for defining a parameterized uniform / Epanechnikov CDF.
    """

    def __init__(self, level: int, size: int, scale: int, epsilon: int) -> None:
        """
        Initialize the kernel.

        Parameters
        ----------
        level : int
            The level used to set the horizontal component of the mean.
        size : int
            The size used to set the vertical component of the mean.
        scale : int
            The scale parameter used to set the support of the (vertical) Epanechnikov kernel.
            The support is defined as: (`size`-`scale`, `size`+`scale`).
        epsilon : int
            The scale parameter used to set the support of the (horizontal) uniform kernel.
            The support is defined as: (`level`-`epsilon`, `level`+`epsilon`).
        """
        self.size = size
        self.scale = scale
        self.level = level
        self.epsilon = epsilon

        self.term = 1 / 2
        self.factor1 = 3 / (4 * scale)
        self.factor2 = 1 / (4 * scale**3)

    def __apply_epanechnikov_cdf(self, x: np.ndarray) -> np.ndarray:
        """
        A helper function for calculating the Epanechnikov part of the kernel.

        Parameters
        ----------
        x : np.ndarray
            The values to evaluate the Epanechnikov CDF at.

        Returns
        -------
        np.ndarray
            The result of mapping `x` through the Epanechnikov CDF.
        """
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
        """
        Apply the uniform / Epanechnikov CDF to `x`.

        Parameters
        ----------
        x : np.ndarray
            The values to evaluate the uniform / Epanechnikov CDF kernel at.

        Returns
        -------
        np.ndarray
            The result of mapping `x` through the uniform / Epanechnikov CDF kernel.
        """
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
    """
    A class for defining a parameterized uniform / uniform CDF.
    """

    def __init__(self, level: int, size: int, scale: int, epsilon: int) -> None:
        """
        Initialize the kernel.

        Parameters
        ----------
        level : int
            The level used to set the horizontal component of the mean.
        size : int
            The size used to set the vertical component of the mean.
        scale : int
            The scale parameter used to set the support of the (vertical) uniform kernel.
            The support is defined as: (`size`-`scale`, `size`+`scale`).
        epsilon : int
            The scale parameter used to set the support of the (horizontal) uniform kernel.
            The support is defined as: (`level`-`epsilon`, `level`+`epsilon`).
        """
        self.size = size
        self.scale = scale
        self.level = level
        self.epsilon = epsilon

        self.slope = 1 / (2 * scale)

    def __apply_uniform_cdf(self, x: np.ndarray) -> np.ndarray:
        """
        A helper function for calculating the vertical uniform part of the kernel.

        Parameters
        ----------
        x : np.ndarray
            The values to evaluate the uniform CDF at.

        Returns
        -------
        np.ndarray
            The result of mapping `x` through the uniform CDF.
        """
        y = np.empty_like(x)
        x_centered = x - self.size
        lower_mask = x_centered <= -self.scale
        upper_mask = x_centered >= self.scale
        inner_mask = np.logical_not(
            np.logical_or(lower_mask, upper_mask)
        )  # The complement of the union of the lower and upper masks
        y[lower_mask] = np.zeros_like(x[lower_mask])
        y[upper_mask] = np.ones_like(x[upper_mask])
        y[inner_mask] = self.slope * (x_centered[inner_mask] + self.scale)
        return y

    def __call__(self, x: np.ndarray) -> np.ndarray:
        """
        Apply the uniform / uniform CDF to `x`.

        Parameters
        ----------
        x : np.ndarray
            The values to evaluate the uniform / uniform CDF kernel at.

        Returns
        -------
        np.ndarray
            The result of mapping `x` through the uniform / uniform CDF kernel.
        """
        measure = np.zeros((x.shape[0], 1))
        mask = np.logical_and(
            x[:, 0] > self.level - self.epsilon, x[:, 0] < self.level + self.epsilon
        )  # Mask everything outside our uniform distribution
        measure[np.expand_dims(mask, 1)] = self.__apply_uniform_cdf(x[:, 1][mask]) / (
            2 * self.epsilon
        )  # The calculation for the Epanechnikov cdf kernel, reweighed by the width of the uniform distribution
        return measure.squeeze()


# def kde_via_sampling(
#     kernels: (
#         list[parameterized_diagonal_multivariate_gaussian]
#         | list[parameterized_uniform_gaussian]
#     ),
#     data_points: np.ndarray,
#     kernel_weights: np.ndarray,
#     coordinates: np.ndarray,
#     resolution: int,
#     weight_type: str,
# ) -> np.ndarray:
#     """
#     A function for calculating the kde of the (weighted) mean of `kernels` evaluated at `coordinates`.

#     Parameters
#     ----------
#     kernels : list[parameterized_diagonal_multivariate_gaussian]  |  list[parameterized_uniform_gaussian]
#         A list of kernels to use for kde.
#     data_points : np.ndarray
#         The data points may be used to weight the kernels, depending on the setting of `weight_type`.
#     kernel_weights : np.ndarray
#         These values are used to calculate a weighted mean over the kernels.
#     coordinates : np.ndarray
#         The coordinates at which we will evaluate the kernels.
#         The amount of coordinates will determine the amount of pixels in the final kde plot.
#     resolution : int
#         The resolution of the kde plot.
#         Note that we assume a square (i.e. `resolution` x `resolution`) plot.
#     weight_type : str
#         One of "block_based" or "vertex_based".
#         The "block_based" option only uses `kernel_weights`, while the "vertex_based" option also multiplies by `data_points[:, 1]`.

#     Returns
#     -------
#     np.ndarray
#         An (`resolution` x `resolution`) array, containing the weighted mean of the kde `kernels` evaluated at `coordinates`.

#     Raises
#     ------
#     ValueError
#         `weight_type` should be set to one of: "block_based" or "vertex_based"
#     """
#     # Calculate the average, weighted kernel
#     # The rule `running_mean*(weight/new_weight) + image(current_weight/new_weight)` prevents values from exploding or disappearing
#     weight = 0
#     data_point_count = len(kernels)
#     running_mean = np.zeros((resolution**2), dtype=np.float64)
#     for i in tqdm(range(data_point_count), desc="Calculating average of kernels"):
#         image = kernels[i](coordinates)
#         if weight_type == "block_based":
#             current_weight = kernel_weights[i]
#         elif weight_type == "vertex_based":
#             current_weight = kernel_weights[i] * data_points[i][1]
#         else:
#             raise ValueError(
#                 '`weight_type` should be set to one of: "block_based" or "vertex_based"'
#             )
#         new_weight = weight + current_weight
#         running_mean = running_mean * (weight / new_weight) + image * (
#             current_weight / new_weight
#         )
#         weight = new_weight
#     return running_mean.reshape(resolution, resolution).T


def kde_via_integration(
    kernels: list[KernelCDFClass],
    data_points: np.ndarray,
    kernel_weights: np.ndarray,
    coordinates: np.ndarray,
    resolution: int,
    weight_type: str,
) -> np.ndarray:
    """
    A function for calculating the kde of the (weighted) mean of `kernels` by evaluating the CDF at `coordinates` and then computing the definite integral.

    Parameters
    ----------
    kernels : list[KernelCDFClass]
        A list of kernels to use for kde.
    data_points : np.ndarray
        The data points may be used to weight the kernels, depending on the setting of `weight_type`.
    kernel_weights : np.ndarray
        These values are used to calculate a weighted mean over the kernels.
    coordinates : np.ndarray
        The coordinates at which we will evaluate the CDFs of the kernels.
        The amount of coordinates will determine the amount of pixels in the final kde plot.
        Note that by computing the definite integral, we will have one less vertical value in the final output.
    resolution : int
        The resolution of the kde plot.
        Note that we assume a square (i.e. `resolution` x `resolution`) plot.
    weight_type : str
        One of "block_based" or "vertex_based".
        The "block_based" option only uses `kernel_weights`, while the "vertex_based" option also multiplies by `data_points[:, 1]`.

    Returns
    -------
    np.ndarray
        An (`resolution` x `resolution`) array, containing the weighted mean of the kde `kernels` by calculating the definite integral between subsequent `coordinates` on the CDF.

    Raises
    ------
    ValueError
        `weight_type` should be set to one of: "block_based" or "vertex_based"
    """
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
        # new_weight = weight + current_weight
        # running_mean = running_mean * (weight / new_weight) + image * (
        #     current_weight / new_weight
        # )
        # weight = new_weight
        running_mean += current_weight*image
    return running_mean


def generic_universal_kde_via_integral_plot(
    data_points: np.ndarray,
    result_directory: str,
    kernel_cdf_generator: Type[KernelCDFClass],
    generator_args: ArgsType = (),
    generator_kwargs: KwargsType = {},
    maximum_level: int | None = None,
    resolution: int = 512,
    weight_type: str = "vertex_based",
    log_size: bool = True,
    log_base: float = 10,
    log_heatmap=True,
    mark_smallest=True,
    clip: float = 0.0,
    clip_removes=False,
    plot_name="block_sizes_integral_kde",
) -> None:
    """
    Make a kde plot of `data_points`.

    Parameters
    ----------
    data_points : np.ndarray
        The data to make the kde plot from.
    result_directory : str
        The director in which to store the plot.
    kernel_cdf_generator : Type[KernelCDFClass]
        A class that parameterizes a callable CDF kernel.
    generator_args : ArgsType, optional
        The optional positional arguments of the passed `kernel_cdf_generator`, by default ().
    generator_kwargs : KwargsType, optional
        The optional keyword arguments of the passed `kernel_cdf_generator`, by default {}.
    maximum_level : int | None, optional
        The possibly pre-computed maximum_level. If set to None it default to `np.max(data_points[:, 0])`.
        By default None.
    resolution : int, optional
        The resolution of the (square) plot, by default 512.
    weight_type : str, optional
        This determines how the weighted mean of kde kernels is calculated.
        By default "vertex_based".
    log_size : bool, optional
        This indicates whether the sizes (vertical axis) should be plotted on log-scale, by default True.
    log_base : float, optional
       The base of logarithm used for `log_size` and `log_heatmap`, by default 10.
    log_heatmap : bool, optional
        This indicates whether the heatmap (colours) should be plotted on log-scale, by default True.
    mark_smallest : bool, optional
        If log_heatmap is set to `True`, then this determines whether the colorbar of the heatmap should have a tick marking the smallest non-zero value, by default True.
    clip : float, optional
        This determines which fraction of the largest values are clipped.
        Ranging from clipping no values at 0.0 to clipping all values at 1.0.
        By default 0.0.
    clip_removes : bool, optional
        This indicates whether the clipped values should be set to 1.0 (False) or removed entirely from the plot (True).
        By default False.
    plot_name : str, optional
        The name of the file in which the figure will be saved.
        By default "block_sizes_integral_kde".

    Raises
    ------
    ValueError
        The maximum_level parameter should be set to at least 0 (or None)
    ValueError
        The resolution parameter should be at least 1
    ValueError
        The weight_type parameter should be set to one of `ACCEPTABLE_WEIGHT_TYPES`
    ValueError
        The log_base parameter should be greater than 1
    ValueError
        The clip parameter should be between 0.0 and 1.0
    """
    SIZE_PADDING = 0.2
    ACCEPTABLE_WEIGHT_TYPES = {"block_based", "vertex_based"}

    # >>> Check for insane inputs >>>
    if maximum_level is not None and maximum_level < 0:
        raise ValueError(
            f"The maximum_level parameter (set to {maximum_level}) should be set to at least 0 (or None)"
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

    if maximum_level is None:
        maximum_level = np.max(data_points[:, 0])

    maximum_size = int(data_points[:, 1].max())
    if weight_type == "vertex_based":
        weights = data_points[:, 1]*data_points[:, 2]
    elif weight_type == "block_based":
        weights = data_points[:, 2]
    heatmap_weight = int(weights.max())
    smallest_non_zero = np.min(weights[np.nonzero(weights)])  # In principle, there should not be zero-weights, but just in case we filter out the potential zeros
    
    # Process the data (e.g. normalize the means)
    means, kernel_weights = means_weights_from_data(data_points)

    data_point_count = data_points.shape[0]
    kernel_CDFs = []
    for i in tqdm(range(data_point_count), desc="Setting up kernels............"):
        kernel_CDFs.append(
            kernel_cdf_generator(
                means[i][0], means[i][1], *generator_args, **generator_kwargs
            )
        )

    levels = np.arange(resolution) / (resolution - 1)
    levels = levels * ((maximum_level + 1) / maximum_level) - 1 / (
        2 * maximum_level
    )  # Adust the range, such that the first and last levels are displayed in full (not half)

    # We will require `resolution+1` coordinates, since we will be using a definite integral to get the final `resolution` pixel values
    if not log_size:
        sizes = np.arange(resolution + 1) / (resolution)
    else:
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

    # kde /= np.max(kde)  # Normalize

    if clip > 0.0:
        clip_threshold = np.max(kde)*(1-clip)
        kde[kde > clip_threshold] = 0 if clip_removes else clip_threshold  # Clip "large" values
    #     kde /= np.max(
    #         kde
    #     )  # Renomalize (note that if clip_removes == True, then np.max(kde) == 1-clip)
    
    # kde *= heatmap_weight  # Scale to the original counts

    fig, ax = plt.subplots()

    x_tick_labels = range(maximum_level + 1)
    x_tick_positions = [
        label * (resolution - 1) / maximum_level for label in x_tick_labels
    ]
    x_tick_positions = [
        position * (maximum_level / (maximum_level + 1))
        + resolution / (2 * (maximum_level + 1))
        for position in x_tick_positions
    ]  # Since we rescaled the levels earlier, we will have to change the ticks accordingly

    # If we have more than 10 ticks, we will only select 6
    TICK_COUNT_CUTOFF = 10
    tick_count = len(x_tick_positions)
    if tick_count > TICK_COUNT_CUTOFF:
        x_tick_indices = np.round(
            np.linspace(0, tick_count - 1, num=6, endpoint=True)
        ).astype(int)
        x_tick_positions = [x_tick_positions[i] for i in x_tick_indices]
        x_tick_labels = [x_tick_labels[i] for i in x_tick_indices]
    ax.set_xticks(x_tick_positions, x_tick_labels)

    if not log_size:
        y_tick_count = 6
        y_tick_labels = [
            int(round(i * maximum_size / (y_tick_count - 1)))
            for i in range(y_tick_count)
        ]
        y_tick_positions = [
            label * (resolution - 1) / maximum_size for label in y_tick_labels
        ]
        ax.set_yticks(y_tick_positions, y_tick_labels)
    else:
        log_range = log(maximum_size, log_base) + 2 * SIZE_PADDING
        log_offset = SIZE_PADDING / log_range
        log_scale = log(maximum_size, log_base) / log_range

        y_tick_count = int(floor(log(maximum_size, log_base) + SIZE_PADDING + 1))
        y_tick_positions = np.asarray(
            [i for i in range(y_tick_count)], dtype=np.float64
        ) / log(maximum_size, log_base)

        y_tick_positions *= log_scale
        y_tick_positions += log_offset
        y_tick_positions *= resolution

        y_tick_labels = [f"${log_base}^{i}$" for i in range(y_tick_count)]
        y_tick_positions, y_tick_labels = add_minor_log_ticks(
            y_tick_positions,
            y_tick_labels,
            base=log_base,
            start=None,
            end=resolution - 1,
        )

        ax.set_yticks(y_tick_positions, y_tick_labels)

    # Label the axes
    ax.set_xlabel("Bisimulation level")
    ax.set_ylabel("Block size")

    # Set the title
    log_size_string = "LOG " if log_size else ""
    log_heatmap_string = "LOG " if log_heatmap else ""
    if weight_type == "block_based":
        plot_type_string = "block count of given size"
    elif weight_type == "vertex_based":
        plot_type_string = "vertex count in blocks of given size"
    ax.set_title(f"{log_heatmap_string}Heatmap of {log_size_string}{plot_type_string}")

    # Set the norm for the colorbar later
    norm = None
    if log_heatmap:
        LINEAR_THRESHOLD = 1
        norm = SymLogNorm(linthresh=LINEAR_THRESHOLD)

    # Plot our data as a heatmap
    kde += 0.0
    heatmap = ax.imshow(kde, origin="lower", cmap=PARADISO, interpolation="nearest", norm=norm)
    cbar = fig.colorbar(heatmap)
    if log_heatmap and mark_smallest:
        # Add an extra tick, indicating the smallest non-zero value
        cbar.ax.yaxis.set_major_formatter(LogFormatterSciNotation())
        smallest_non_zero_label = "      Smallest"
        ticks = cbar.get_ticks()
        ticks = np.append(ticks, smallest_non_zero)  # Append the special tick for the smallest non-zero value
        cbar.set_ticks(ticks)
        cbar.ax.set_yticklabels([*map(cbar.ax.yaxis.get_major_formatter().__call__, ticks[:-1]), smallest_non_zero_label])  # Keep original formatting for existing ticks, with a special label for the new one
        
        # Make the tick label gray
        smallest_non_zero_index = len(ticks)-1
        cbar.ax.get_yticklabels()[smallest_non_zero_index].set_color("gray")

        # Make the tick mark gray
        ticks_obj = cbar.ax.yaxis.get_major_ticks()
        ticks_obj[smallest_non_zero_index]._apply_params(color="gray")
    
    # Make (the outside of) the figure transparant
    fig.patch.set_visible(False)
    ax.patch.set_visible(False)

    fig.savefig(result_directory + plot_name + ".svg", dpi=resolution, bbox_inches="tight", pad_inches=0.01)
    fig.savefig(result_directory + plot_name + ".pdf", dpi=resolution, bbox_inches="tight", pad_inches=0.01)


# def generic_universal_kde_plot(
#     data_points: np.ndarray,
#     experiment_directory: str,
#     fixed_point: int | None = None,
#     resolution: int = 512,
#     gaussian_kernels: bool = False,
#     variance: float = 0.01,
#     scale: float = 0.75,
#     weight_type: str = "vertex_based",
#     linear_sizes: bool = False,
#     log_base: int = 10,
#     epsilon: float = 0.5,
#     padding: float = 0.05,
#     clip: float = 0.95,
#     clip_removes=False,
#     **kwargs,
# ) -> None:
#     SIZE_PADDING = 0.2

#     if fixed_point is None:
#         fixed_point = np.max(data_points[:, 0])
#     # Process the data (e.g. normalize the means)
#     maximum_size = int(data_points[:, 1].max())
#     means, kernel_weights = means_weights_from_data(data_points)

#     scale /= maximum_size
#     epsilon = (
#         (1 - padding) * epsilon / fixed_point
#     )  # Using (1 - padding) allows for the regions of adjacent levels not to overlap

#     data_point_count = data_points.shape[0]

#     if gaussian_kernels:
#         standard_deviation = sqrt(variance)
#         # Create all the separate Gaussian kernels, using the specified means
#         kernels = []
#         for i in tqdm(range(data_point_count), desc="Setting up kernels............"):
#             kernels.append(
#                 parameterized_uniform_gaussian(
#                     means[i][0], means[i][1], standard_deviation, epsilon=epsilon
#                 )
#             )
#     else:
#         kernels = []
#         for i in tqdm(range(data_point_count), desc="Setting up kernels............"):
#             kernels.append(
#                 parameterized_uniform_epanechnikov(
#                     means[i][0], means[i][1], scale=scale, epsilon=epsilon
#                 )
#             )

#     levels = np.arange(resolution) / (resolution - 1)
#     levels = levels * ((fixed_point + 1) / fixed_point) - 1 / (
#         2 * fixed_point
#     )  # Adust the range, such that the first and last levels are displayed in full (not half)

#     if linear_sizes:
#         sizes = np.arange(resolution) / (resolution - 1)
#     else:
#         sizes = np.logspace(
#             0 - SIZE_PADDING,
#             log(maximum_size, log_base) + SIZE_PADDING,
#             num=resolution,
#             endpoint=True,
#             base=log_base,
#         ) / (
#             log_base ** (log(maximum_size, log_base) + SIZE_PADDING)
#             - log_base ** (0 - SIZE_PADDING)
#         )  # Logarithmically scaled numbers from 0 to 1

#     coordinates = np.array(np.meshgrid(levels, sizes)).T.reshape(-1, 2)

#     kde = kde_via_sampling(
#         kernels, data_points, kernel_weights, coordinates, resolution, weight_type
#     )

#     kde /= np.max(kde)  # Normalize
#     if not linear_sizes:
#         kde[kde > 1 - clip] = 0 if clip_removes else 1 - clip  # Clip "large" values
#         kde /= np.max(
#             kde
#         )  # Renomalize (note that if clip_removes == True, then np.max(kde) == 1-clip)

#     x_tick_labels = range(fixed_point + 1)
#     x_tick_positions = [
#         label * (resolution - 1) / fixed_point for label in x_tick_labels
#     ]
#     x_tick_positions = [
#         position * (fixed_point / (fixed_point + 1))
#         + resolution / (2 * (fixed_point + 1))
#         for position in x_tick_positions
#     ]  # Since we rescaled the levels earlier, we will have to change the ticks accordingly
#     plt.xticks(x_tick_positions, x_tick_labels)

#     if linear_sizes:
#         y_tick_count = 6
#         y_tick_labels = [
#             int(round(i * maximum_size / (y_tick_count - 1)))
#             for i in range(y_tick_count)
#         ]
#         y_tick_positions = [
#             label * (resolution - 1) / maximum_size for label in y_tick_labels
#         ]
#         plt.yticks(y_tick_positions, y_tick_labels)
#     else:
#         start = 1 - log_base ** (-SIZE_PADDING)
#         log_maximum_size = log(maximum_size, log_base)
#         log_maximum_size_padded = log(maximum_size + SIZE_PADDING, log_base)
#         y_tick_count = int(floor(log_maximum_size_padded)) + 1
#         y_tick_positions = (
#             np.asarray([i for i in range(y_tick_count)], dtype=np.float64)
#             / y_tick_count
#         )
#         y_tick_positions = (y_tick_positions * (log_maximum_size - start) + start) * (
#             1 / log_maximum_size_padded
#         )
#         y_tick_positions *= resolution
#         y_tick_labels = [f"${log_base}^{i}$" for i in range(y_tick_count)]
#         y_tick_positions, y_tick_labels = add_minor_log_ticks(
#             y_tick_positions, y_tick_labels, base=log_base, start=0, end=resolution - 1
#         )
#         plt.yticks(y_tick_positions, y_tick_labels)

#     # Label the axes
#     plt.xlabel("Bisimulation level")
#     plt.ylabel("Block size")

#     # Plot our data as a heatmap
#     plt.imshow(kde, origin="lower", cmap=PARADISO, interpolation="nearest")
#     plt.savefig(
#         experiment_directory + "results/block_sizes_generic_universal_kde.svg",
#         dpi=resolution,
#     )


# def gaussian_log_log_kde_plot(
#     data_points: np.ndarray,
#     experiment_directory: str,
#     fixed_point: int | None = None,
#     dimension: int = 2,
#     resolution: int = 512,
#     variance_type: str = "scott",
#     variance_factor: float = 0.01,
#     weight_type: str = "vertex_based",
#     log_base: int = 10,
#     **kwargs,
# ) -> None:
#     if fixed_point is None:
#         fixed_point = np.max(data_points[:, 0])

#     # Process the data (e.g. normalize the means)
#     maximum_size = int(data_points[:, 1].max())
#     means, kernel_weights = means_weights_from_data(data_points)

#     # We use `variance_factor` if we want to manually scale the variance
#     data_point_count = data_points.shape[0]
#     standard_deviations = determine_standard_deviation(
#         data_point_count, dimension, variance_type
#     ) * sqrt(variance_factor)

#     # Create all the separate Gaussian kernels, using the specified means
#     kernels = []
#     for i in tqdm(range(data_point_count), desc="Setting up kernels............"):
#         kernels.append(
#             parameterized_diagonal_multivariate_gaussian(means[i], standard_deviations)
#         )

#     # Create the coordinates, used for sampling the kde kernels
#     log_sizes = (
#         np.logspace(
#             0, log(maximum_size, log_base), num=resolution, endpoint=True, base=log_base
#         )
#         / maximum_size
#     )  # Logarithmically scaled numbers from 0 to 1
#     log_levels = (
#         np.logspace(
#             0,
#             log(fixed_point + 1, log_base),
#             num=resolution,
#             endpoint=True,
#             base=log_base,
#         )
#         - 1
#     ) / fixed_point  # Logarithmically scaled numbers from 0 to 1
#     log_coordinates = np.array(np.meshgrid(log_levels, log_sizes)).T.reshape(-1, 2)

#     # Get the final result from sampling our kernels at the given coordinates
#     kde = kde_via_sampling(
#         kernels, data_points, kernel_weights, log_coordinates, resolution, weight_type
#     )

#     # Set the x and y ticks differently for small numbers (less that log_base) than for big numbers
#     if fixed_point < log_base:
#         x_tick_positions = np.asarray(
#             [log(i, log_base) for i in range(1, fixed_point + 2)]
#         )
#         x_tick_positions = x_tick_positions * (
#             (resolution - 1) / float(np.max(x_tick_positions))
#         )
#         x_tick_labels = [f"{i}" for i in range(fixed_point + 1)]
#         plt.xticks(x_tick_positions, x_tick_labels)
#     else:
#         log_fixed_point = log(fixed_point + 1, log_base)
#         x_tick_count = int(floor(log_fixed_point)) + 1
#         x_tick_positions = np.asarray(
#             [i for i in range(x_tick_count)], dtype=np.float64
#         ) * (resolution / log_fixed_point)
#         x_tick_labels = [f"${log_base}^{i}$" for i in range(x_tick_count)]
#         x_tick_positions, x_tick_labels = add_minor_log_ticks(
#             x_tick_positions, x_tick_labels, base=log_base, resolution=resolution
#         )
#         plt.xticks(x_tick_positions, x_tick_labels)

#     if maximum_size < log_base:
#         y_tick_positions = np.asarray(
#             [log(i, log_base) for i in range(1, maximum_size + 2)]
#         )
#         y_tick_positions = y_tick_positions * (
#             (resolution - 1) / float(np.max(y_tick_positions))
#         )
#         y_tick_labels = [f"{i}" for i in range(maximum_size + 1)]
#         plt.yticks(y_tick_positions, y_tick_labels)
#     else:
#         log_maximum_size = log(maximum_size, log_base)
#         y_tick_count = int(floor(log_maximum_size)) + 1
#         y_tick_positions = np.asarray(
#             [i for i in range(y_tick_count)], dtype=np.float64
#         ) * (resolution / log_maximum_size)
#         y_tick_labels = [f"${log_base}^{i}$" for i in range(y_tick_count)]
#         y_tick_positions, y_tick_labels = add_minor_log_ticks(
#             y_tick_positions, y_tick_labels, base=log_base, start=0, end=resolution - 1
#         )
#         plt.yticks(y_tick_positions, y_tick_labels)

#     # Label the axes
#     plt.xlabel("Bisimulation level")
#     plt.ylabel("Block size")

#     # Plot our data as a heatmap
#     plt.imshow(kde, origin="lower", cmap=PARADISO, interpolation="nearest")
#     plt.savefig(
#         experiment_directory + "results/block_sizes_gaussian_log_log_kde.svg",
#         dpi=resolution,
#     )


if __name__ == "__main__":
    experiment_directory = sys.argv[1]

    # Load in the data
    fixed_point = get_fixed_point(experiment_directory)
    block_sizes = get_sizes_and_split_blocks(experiment_directory, fixed_point)
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

    # # Create a multivariate Gaussian kde plot, shown in log-log scale
    # gaussian_log_log_kwargs = {
    #     "dimension": 2,
    #     "resolution": int(512 * 2**0),
    #     "variance_type": "scott",
    #     "variance_factor": 0.001,
    #     "weight_type": "vertex_based",
    #     "log_base": 10,
    # }
    # gaussian_log_log_kde_plot(
    #     data_points, experiment_directory, fixed_point, **gaussian_log_log_kwargs
    # )
    # # plt.show()

    # # Create a 2D (Gaussian or Epanechnikov)-universal kde plot, shown in lin-log scale
    # generic_universal_kwargs = {
    #     "resolution": 512,
    #     "gaussian_kernels": False,
    #     "variance": 0.001,
    #     "scale": 0.5,
    #     "weight_type": "vertex_based",
    #     "linear_sizes": False,
    #     "log_base": 10,
    #     "epsilon": 0.5,
    #     "padding": 0.05,
    #     "clip": 0.90,
    #     "clip_removes": False,
    # }
    # generic_universal_kde_plot(
    #     data_points, experiment_directory, fixed_point, **generic_universal_kwargs
    # )
    # # plt.show()

    # Create a 2D (Gaussian or Epanechnikov or custom)-universal kde plot that uses integration instead of sampling to get the heatmap values, shown in lin-log scale
    via_integration_kwargs = {
        "resolution": 512,
        "weight_type": "block_based",
        "log_size": True,
        "log_base": 10,
        "log_heatmap": True,
        "mark_smallest":True,
        "clip": 0.00,
        "clip_removes": False,
        "plot_name": "block_sizes_integral_kde",
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

    result_directory = experiment_directory + "results/"

    generic_universal_kde_via_integral_plot(
        data_points,
        result_directory,
        UniformCDF,
        epanechnikov_args,
        epanechnikov_kwargs,
        fixed_point,
        **via_integration_kwargs,
    )
    # plt.show()
