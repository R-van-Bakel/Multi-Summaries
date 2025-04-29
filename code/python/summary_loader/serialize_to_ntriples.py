import hashlib
import base64
import sys
import os
from collections import Counter
from typing import Protocol
from urllib.parse import quote
from loader_functions import (
    get_local_global_maps,
    get_fixed_point,
    get_node_intervals,
    BYTES_PER_ENTITY,
    BYTES_PER_PREDICATE,
    BYTES_PER_BLOCK,
    BYTES_PER_BLOCK_OR_SINGLETON,
    BYTES_PER_K_TYPE,
)

TRUNCATE = False
# NAMESPACE = "http://www.vu.nl/test#"
NAMESPACE = "http://cs.vu.nl/clustering#"
TEST_STRING = "hello world"


class BlockIDListToStringMapperClass(Protocol):
    """
    A protocol type used for type hints.
    """

    def __call__(self, id_list: list[int]) -> str:
        """
        Map a list of ids to a string

        Parameters
        ----------
        id_list : list[int]
            A list of integer ids

        Returns
        -------
        str
            A string used to represent the id list
        """
        ...


def parse_to_iri(unsafe_string: str) -> str:
    return quote(unsafe_string, safe=":/?#[]@!$&'()*+,;=")


class SortedIDMapper:
    def __call__(self, id_list: list[int]) -> str:
        sorted_id_string = ",".join([str(entity_id) for entity_id in sorted(id_list)])
        return NAMESPACE + parse_to_iri("id_block-{" + sorted_id_string + "}")


class SortedIRIMapper:
    def __init__(self, id_to_entity_map: dict[int, str]) -> None:
        self.id_to_entity_map = id_to_entity_map

    def __call__(self, id_list: list[int]) -> str:
        sorted_iri_string = ",".join(
            sorted([self.id_to_entity_map[entity_id] for entity_id in id_list])
        )
        return NAMESPACE + parse_to_iri("iri_block-{" + sorted_iri_string + "}")


class SortedIRIHashMapper:
    def __init__(self, id_to_entity_map: dict[int, str]) -> None:
        self.id_to_entity_map = id_to_entity_map

    def __call__(self, id_list: list[int]) -> str:
        sorted_iri_string = ",".join(
            sorted([self.id_to_entity_map[entity_id] for entity_id in id_list])
        )
        iri_hash_bytes = hashlib.sha256(sorted_iri_string.encode("utf-8")).digest()
        iri_hash_bytes = iri_hash_bytes[:16] if TRUNCATE else iri_hash_bytes
        iri_hash_string = base64.b64encode(iri_hash_bytes).decode("utf-8")
        return NAMESPACE + parse_to_iri("hash_block-" + iri_hash_string)


def get_id_entity_maps(
    experiment_directory: str, include_inverted_index=False
) -> tuple[dict[int, str], dict[str, int]]:
    assert os.path.exists(
        experiment_directory
    ), "The experiment directory string should refer to a valid (existing) directory"
    entity_to_id_map_file = experiment_directory + "entity2ID.txt"

    id_to_entity_map = dict()
    entity_to_id_map = dict()

    with open(entity_to_id_map_file, "r", encoding="utf-8") as f:
        for line_number, line in enumerate(f, start=1):
            try:
                entity_iri, entity_id = line.split(" ")
            except ValueError as e:
                print(
                    f"Error in splitting line in get_id_entity_maps(): The IRI in line {line_number} likely containts spaces",
                    file=sys.stderr,
                )
                raise
            entity_id = int(entity_id)

            id_to_entity_map[entity_id] = entity_iri
            if include_inverted_index:
                entity_to_id_map[entity_iri] = entity_id

    return id_to_entity_map, entity_to_id_map


def get_id_predicate_maps(
    experiment_directory: str, include_inverted_index=False
) -> tuple[dict[int, str], dict[str, int]]:
    assert os.path.exists(
        experiment_directory
    ), "The experiment directory string should refer to a valid (existing) directory"
    entity_to_id_map_file = experiment_directory + "rel2ID.txt"

    id_to_predicate_map = dict()
    predicate_to_id_map = dict()

    with open(entity_to_id_map_file, "r", encoding="utf-8") as f:
        for line_number, line in enumerate(f, start=1):
            try:
                predicate_iri, predicate_id = line.split(" ")
            except ValueError as e:
                print(
                    f"Error in splitting line in get_id_predicate_maps(): The IRI in line {line_number} likely containts spaces",
                    file=sys.stderr,
                )
                raise
            predicate_id = int(predicate_id)

            id_to_predicate_map[predicate_id] = predicate_iri
            if include_inverted_index:
                predicate_to_id_map[predicate_iri] = predicate_id

    return id_to_predicate_map, predicate_to_id_map


def process_outcomes(
    experiment_directory: str,
    depth: int,
    local_to_global_block_map: dict[tuple[int, int], int],
    block_to_string_mapper: BlockIDListToStringMapperClass,
    id_to_entity_map: dict[int, str],
) -> dict[int, str]:
    CONTAINS_IRI = NAMESPACE + "contains"
    SIZE_IRI = NAMESPACE + "size"
    assert os.path.exists(
        experiment_directory
    ), "The experiment directory string should refer to a valid (existing) directory"

    rdf_output_directory = experiment_directory + "rdf_summary_graph/"
    assert os.path.exists(
        rdf_output_directory
    ), "The output directory string should refer to a valid (existing) directory"
    block_sizes_file_path = rdf_output_directory + "sizes.nt"
    block_contains_file_path = rdf_output_directory + "contains.nt"

    global_id_to_iri_map = dict()

    with open(block_sizes_file_path, "w") as block_sizes_file, open(
        block_contains_file_path, "w"
    ) as block_contains_file:
        for level in range(depth + 1):
            outcome_binary_file = (
                experiment_directory + f"bisimulation/outcome_condensed-{level:04d}.bin"
            )
            assert os.path.isfile(
                outcome_binary_file
            ), "The outcome (binary) file should exist"

            if level >= 1:
                singleton_mapping_binary_file = (
                    experiment_directory
                    + f"bisimulation/singleton_mapping-{level-1:04d}to{level:04d}.bin"
                )
                # If singletons were created, then the singleton_mapping file contains this information
                if os.path.isfile(singleton_mapping_binary_file):
                    with open(singleton_mapping_binary_file, "rb") as f:
                        while merged_bytes := f.read(BYTES_PER_BLOCK):
                            _ = int.from_bytes(merged_bytes, "little", signed=False)
                            singleton_count = int.from_bytes(
                                f.read(BYTES_PER_BLOCK_OR_SINGLETON),
                                "little",
                                signed=True,
                            )
                            for singleton in range(singleton_count):
                                singleton_id = int.from_bytes(
                                    f.read(BYTES_PER_BLOCK_OR_SINGLETON),
                                    "little",
                                    signed=True,
                                )
                                singleton_entity_id = -singleton_id - 1
                                global_id_to_iri_map[singleton_id] = (
                                    block_to_string_mapper([singleton_entity_id])
                                )
                                block_sizes_file.write(
                                    f'<{global_id_to_iri_map[singleton_id]}> <{SIZE_IRI}> "1" .\n'
                                )
                                entity_string = id_to_entity_map[singleton_entity_id]
                                if not entity_string.startswith("_:"):
                                    entity_string = "<" + entity_string + ">"
                                block_contains_file.write(
                                    f"<{global_id_to_iri_map[singleton_id]}> <{CONTAINS_IRI}> {entity_string} .\n"
                                )

            with open(outcome_binary_file, "rb") as f:
                while block_bytes := f.read(BYTES_PER_BLOCK):
                    block_id = int.from_bytes(block_bytes, "little", signed=False)
                    block_size = int.from_bytes(
                        f.read(BYTES_PER_ENTITY), "little", signed=False
                    )

                    entity_id_list = []
                    for i in range(block_size):
                        entity = int.from_bytes(
                            f.read(BYTES_PER_ENTITY), "little", signed=False
                        )
                        entity_id_list.append(entity)
                    global_block_id = local_to_global_block_map[(block_id, level)]
                    global_id_to_iri_map[global_block_id] = block_to_string_mapper(
                        entity_id_list
                    )

                    block_sizes_file.write(
                        f'<{global_id_to_iri_map[global_block_id]}> <{SIZE_IRI}> "{block_size}" .\n'
                    )
                    for entity_id in entity_id_list:
                        entity_string = id_to_entity_map[entity_id]
                        if not entity_string.startswith("_:"):
                            entity_string = "<" + entity_string + ">"
                        block_contains_file.write(
                            f"<{global_id_to_iri_map[global_block_id]}> <{CONTAINS_IRI}> {entity_string} .\n"
                        )

    return global_id_to_iri_map


def load_and_store_refines_edges(
    experiment_directory: str,
    depth: int,
    local_to_global_block_map: dict[tuple[int, int], int],
    global_id_to_iri_map: dict[int, str],
    typed_start=True,
) -> None:
    REFINES_IRI = NAMESPACE + "refines"
    assert os.path.exists(
        experiment_directory
    ), "The experiment directory string should refer to a valid (existing) directory"

    rdf_output_directory = experiment_directory + "rdf_summary_graph/"
    assert os.path.exists(
        rdf_output_directory
    ), "The output directory string should refer to a valid (existing) directory"
    refines_edges_file = rdf_output_directory + "refines.nt"
    block_to_start_map = dict()
    start = (
        0 if typed_start else 1
    )  # The untyped start has a trivial universal block at level 0, so it will not be stored explicitly

    outcome_binary_file = (
        experiment_directory + f"bisimulation/outcome_condensed-{start:04d}.bin"
    )
    with open(outcome_binary_file, "rb") as infile:
        while block_bytes := infile.read(BYTES_PER_BLOCK):
            block_id = int.from_bytes(block_bytes, "little", signed=False)
            block_size = int.from_bytes(
                infile.read(BYTES_PER_ENTITY), "little", signed=False
            )
            block_to_start_map[block_id] = start
            infile.seek(block_size * BYTES_PER_ENTITY, 1)

    with open(refines_edges_file, "w") as outfile:
        for i in range(start + 1, depth + 1):
            mapping_binary_file = (
                experiment_directory + f"bisimulation/mapping-{i-1:04d}to{i:04d}.bin"
            )
            assert os.path.isfile(
                mapping_binary_file
            ), "The mapping (binary) file should exist"

            singleton_mapping_binary_file = (
                experiment_directory
                + f"bisimulation/singleton_mapping-{i-1:04d}to{i:04d}.bin"
            )
            # If singletons were created, then the singleton_mapping file contains this information
            if os.path.isfile(singleton_mapping_binary_file):
                with open(singleton_mapping_binary_file, "rb") as infile:
                    while merged_bytes := infile.read(BYTES_PER_BLOCK):
                        merged_id = int.from_bytes(merged_bytes, "little", signed=False)
                        global_merged_id = local_to_global_block_map[
                            (merged_id, block_to_start_map[merged_id])
                        ]
                        singleton_count = int.from_bytes(
                            infile.read(BYTES_PER_BLOCK_OR_SINGLETON),
                            "little",
                            signed=True,
                        )
                        for singleton in range(singleton_count):
                            singleton_id = int.from_bytes(
                                infile.read(BYTES_PER_BLOCK_OR_SINGLETON),
                                "little",
                                signed=True,
                            )
                            outfile.write(
                                f"<{global_id_to_iri_map[singleton_id]}> <{REFINES_IRI}> <{global_id_to_iri_map[global_merged_id]}> .\n"
                            )

            # For all non-singleton blocks, read the mapping file
            with open(mapping_binary_file, "rb") as infile:
                while merged_bytes := infile.read(BYTES_PER_BLOCK):
                    merged_id = int.from_bytes(merged_bytes, "little", signed=False)
                    global_merged_id = local_to_global_block_map[
                        (merged_id, block_to_start_map[merged_id])
                    ]
                    split_count = int.from_bytes(
                        infile.read(BYTES_PER_BLOCK), "little", signed=False
                    )
                    for split_block in range(split_count):
                        split_id = int.from_bytes(
                            infile.read(BYTES_PER_BLOCK), "little", signed=False
                        )
                        # We handled blocks splitting into singletons (i.e. split_id == 0) already
                        if split_id == 0:
                            continue
                        global_split_id = local_to_global_block_map[(split_id, i)]
                        block_to_start_map[split_id] = i
                        outfile.write(
                            f"<{global_id_to_iri_map[global_split_id]}> <{REFINES_IRI}> <{global_id_to_iri_map[global_merged_id]}> .\n"
                        )


def load_and_store_data_edges(
    experiment_directory: str,
    global_id_to_iri_map: dict[int, str],
    id_to_predicate_map: dict[int, str],
) -> None:
    assert os.path.exists(
        experiment_directory
    ), "The experiment directory string should refer to a valid (existing) directory"
    graph_binary_file = (
        experiment_directory + "bisimulation/condensed_multi_summary_graph.bin"
    )
    assert os.path.isfile(graph_binary_file), "The graph (binary) file should exists"

    assert os.path.exists(
        experiment_directory
    ), "The experiment directory string should refer to a valid (existing) directory"

    rdf_output_directory = experiment_directory + "rdf_summary_graph/"
    assert os.path.exists(
        rdf_output_directory
    ), "The output directory string should refer to a valid (existing) directory"
    data_edges_file = rdf_output_directory + "data.nt"

    with open(graph_binary_file, "rb") as infile, open(data_edges_file, "w") as outfile:
        while subject_bytes := infile.read(BYTES_PER_BLOCK_OR_SINGLETON):
            subject_id = int.from_bytes(subject_bytes, "little", signed=True)
            predicate_id = int.from_bytes(
                infile.read(BYTES_PER_PREDICATE), "little", signed=False
            )
            object_id = int.from_bytes(
                infile.read(BYTES_PER_BLOCK_OR_SINGLETON), "little", signed=True
            )
            outfile.write(
                f"<{global_id_to_iri_map[subject_id]}> <{id_to_predicate_map[predicate_id]}> <{global_id_to_iri_map[object_id]}> .\n"
            )


def store_node_intervals(
    experiment_directory: str,
    node_intervals: dict[int, tuple[int, int]],
    global_id_to_iri_map: dict[int, str],
) -> None:
    START_IRI = NAMESPACE + "startLevel"
    END_IRI = NAMESPACE + "endLevel"
    assert os.path.exists(
        experiment_directory
    ), "The experiment directory string should refer to a valid (existing) directory"

    rdf_output_directory = experiment_directory + "rdf_summary_graph/"
    assert os.path.exists(
        rdf_output_directory
    ), "The output directory string should refer to a valid (existing) directory"
    node_intervals_file = rdf_output_directory + "intervals.nt"

    with open(node_intervals_file, "w") as f:
        for global_node_id, interval in node_intervals.items():
            f.write(
                f'<{global_id_to_iri_map[global_node_id]}> <{START_IRI}> "{interval[0]}" .\n<{global_id_to_iri_map[global_node_id]}> <{END_IRI}> "{interval[0]}" .\n'
            )


if __name__ == "__main__":
    MIN_ARGC = 3
    argc = len(sys.argv)
    if argc < MIN_ARGC:
        raise TypeError(f"Received {argc-1} arguments, but at least two should be provided: 1) the path to an experiment directory, 2) the iri output type (one of: \"id_set\", \"iri_set\", or \"hash\")")
    experiment_directory = sys.argv[1]
    iri_type = sys.argv[2]
    if not iri_type in {"id_set", "iri_set", "hash"}:
        raise ValueError(f"`iri_type` is set to \"{iri_type}\". It should be one of: \"id_set\", \"iri_set\", or \"hash\".")

    rdf_summary_graph_directory = experiment_directory + "rdf_summary_graph/"

    # Make a directory to store the RDF summary graph in
    os.makedirs(
        rdf_summary_graph_directory
    )  # Will raise an error if the directory already exists

    # Load some files
    id_to_entity_map, _ = get_id_entity_maps(experiment_directory)
    id_to_predicate_map, _ = get_id_predicate_maps(experiment_directory)
    local_to_global_block_map, _ = get_local_global_maps(experiment_directory)
    node_intervals = get_node_intervals(experiment_directory)
    fixed_point = get_fixed_point(experiment_directory)

    # Choose a mapper for the IRIs
    if iri_type == "id_set":
        block_to_string_mapper = SortedIDMapper()
    elif iri_type == "iri_set":
        block_to_string_mapper = SortedIRIMapper(id_to_entity_map)
    elif iri_type == "hash":
        block_to_string_mapper = SortedIRIHashMapper(id_to_entity_map) 

    # Create a map from global block ids to IRIs
    # While loading the outcomes, we immediately serialize some data to RDF ntriples files
    print("Making global_id_to_iri_map, while serializing size and contains edges")
    global_id_to_iri_map = process_outcomes(
        experiment_directory,
        fixed_point,
        local_to_global_block_map,
        block_to_string_mapper,
        id_to_entity_map,
    )

    # Store the node intervals
    print("Serializing summary node lifetime intervals")
    store_node_intervals(experiment_directory, node_intervals, global_id_to_iri_map)

    # Load and store the refines edges
    print("Serializing refines edges")
    load_and_store_refines_edges(
        experiment_directory,
        fixed_point,
        local_to_global_block_map,
        global_id_to_iri_map,
        node_intervals,
    )

    # Load and store the data edges
    print("Serializing data edges")
    load_and_store_data_edges(
        experiment_directory, global_id_to_iri_map, id_to_predicate_map
    )
