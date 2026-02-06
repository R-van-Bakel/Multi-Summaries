import sys
import os
import json
import torch
import torch_geometric.nn.conv.rgcn_conv as rgcn_conv

BYTES_PER_ENTITY = 5
BYTES_PER_PREDICATE = 4
BYTES_PER_BLOCK = 4
BYTES_PER_BLOCK_OR_SINGLETON = 5

def summary_vectors_to_original_vectors(node_to_block: list[int], summary_vectors: torch.Tensor, dim: int = 0) -> torch.Tensor:
    return torch.index_select(summary_vectors, dim, torch.as_tensor(node_to_block))

def get_original_graph(experiment_directory) -> tuple[list[list[int]], list[int]]:
    assert os.path.exists(experiment_directory), "The experiment directory string should refer to a valid (existing) directory"
    graph_binary_file = experiment_directory + "binary_encoding.bin"
    assert os.path.isfile(graph_binary_file), "The experiment directory string should refer to a valid (existing) directory"

    edge_index = [[],[]]
    edge_type = []

    with open(graph_binary_file, "rb") as f:
        while (subject_bytes := f.read(BYTES_PER_BLOCK_OR_SINGLETON)):
            subject_id = int.from_bytes(subject_bytes, "little", signed=True)
            predicate_id = int.from_bytes(f.read(BYTES_PER_PREDICATE), "little", signed=False)
            object_id = int.from_bytes(f.read(BYTES_PER_BLOCK_OR_SINGLETON), "little", signed=True)

            edge_index[0].append(subject_id)
            edge_index[1].append(object_id)
            edge_type.append(predicate_id)
    
    return edge_index, edge_type

def get_summary_graph(experiment_directory: str, k: int) -> tuple[list[list[int]], list[int]]:
    assert os.path.exists(experiment_directory), "The experiment directory string should refer to a valid (existing) directory"
    assert k>=1, "k should be an integer greater than or equal to 1"
    summary_graph_file = experiment_directory + f"bisimulation/summary_graph-{k:04}.bin"
    assert os.path.isfile(summary_graph_file), f"There is no summary graph {k}"

    proto_edge_index = [[],[]]
    edge_type = []

    with open(summary_graph_file, "rb") as f:
        while (subject_bytes := f.read(BYTES_PER_BLOCK_OR_SINGLETON)):
            subject_id = int.from_bytes(subject_bytes, "little", signed=True)
            predicate_id = int.from_bytes(f.read(BYTES_PER_PREDICATE), "little", signed=False)
            object_id = int.from_bytes(f.read(BYTES_PER_BLOCK_OR_SINGLETON), "little", signed=True)

            proto_edge_index[0].append(subject_id)
            proto_edge_index[1].append(object_id)
            edge_type.append(predicate_id)
    
    return proto_edge_index, edge_type

def get_outcome(experiment_directory: str, k: int) -> tuple[list[int], list[set[int]]]:
    assert os.path.exists(experiment_directory), "The experiment directory string should refer to a valid (existing) directory"
    assert k>=1, "k should be an integer greater than or equal to 1"
    assert os.path.isfile(experiment_directory + f"bisimulation/outcome_condensed-{k:04}.bin"), f"There is no outcome {k}"
    graph_stats_file = experiment_directory + "ad_hoc_results/graph_stats.json"
    with open(graph_stats_file, "r") as f:
        graph_stats = json.load(f)
        vertex_count = graph_stats["Vertex count"]

    blocks = []
    # Initialize all node as if they all have their own unique block
    node_to_block = list(range(-1,-vertex_count-1, -1))
    
    # Read the first outcome file
    outcome_file = experiment_directory + "bisimulation/outcome_condensed-0001.bin"

    with open(outcome_file, "rb") as f:
        while (block_bytes := f.read(BYTES_PER_BLOCK)):
            block_id = int.from_bytes(block_bytes, "little", signed=False)

            # Expand the blocks list if needed
            if len(blocks) < block_id+1:
                blocks += [set() for i in range(block_id+1-len(blocks))]
            
            block_size = int.from_bytes(f.read(BYTES_PER_ENTITY), "little", signed=False)
            for i in range(block_size):
                node_id = int.from_bytes(f.read(BYTES_PER_ENTITY), "little", signed=False)
                blocks[block_id].add(node_id)
                node_to_block[node_id] = block_id
    
    # Add the initial singleton nodes to block 0
    for node, block in enumerate(node_to_block):
        if block < 0:
            blocks[0].add(node)
    
    # Update the outcome for every new k
    for i in range(1, k):
        mapping_file = experiment_directory + f"bisimulation/mapping-{i:04}to{i+1:04}.bin"
        outcome_file = experiment_directory + f"bisimulation/outcome_condensed-{i+1:04}.bin"

        # Load the refines edges
        refines_edges = {}
        with open(mapping_file, "rb") as f:
            while (split_block_bytes := f.read(BYTES_PER_BLOCK)):
                split_block_id = int.from_bytes(split_block_bytes, "little", signed=False)
                refines_edges[split_block_id] = set()
                new_block_count = int.from_bytes(f.read(BYTES_PER_BLOCK), "little", signed=False)
                for j in range(new_block_count):
                    new_block_id = int.from_bytes(f.read(BYTES_PER_BLOCK), "little", signed=False)
                    refines_edges[split_block_id].add(new_block_id)
        
        # Copy the split blocks for keeping track of singletons
        new_singletons = set()
        for split_block, new_blocks in refines_edges.items():
            if 0 in new_blocks:
                new_singletons.update(blocks[split_block])
            
            # Clear the old block
            blocks[split_block] = set()

        # Update the blocks
        with open(outcome_file, "rb") as f:
            while (block_bytes := f.read(BYTES_PER_BLOCK)):
                block_id = int.from_bytes(block_bytes, "little", signed=False)
                assert block_id != 0, "The outcome should not explicitly encode block 0"  # We do not explicitly store the 0 block (which is reserved for singletons)

                # Expand the blocks list if needed
                if len(blocks) < block_id+1:
                    blocks += [set() for i in range(block_id+1-len(blocks))]

                block_size = int.from_bytes(f.read(BYTES_PER_ENTITY), "little", signed=False)
                for i in range(block_size):
                    node_id = int.from_bytes(f.read(BYTES_PER_ENTITY), "little", signed=False)
                    new_singletons.discard(node_id)  # Since node_id might not be present in new_singletons, we use .discard() instead of .remove()
                    blocks[block_id].add(node_id)
                    node_to_block[node_id] = block_id
        
        # Update the singleton nodes
        for node in new_singletons:
            node_to_block[node] = -node-1
        blocks[0].update(new_singletons)
        
    return node_to_block, blocks

def make_positive_block_node_map(proto_node_to_block: list[int]) -> dict[int,int]:
        block_id_to_proto_block_id = sorted(set(proto_node_to_block))
        proto_block_id_to_block_id = {}
        for i, id in enumerate(block_id_to_proto_block_id):
            proto_block_id_to_block_id[id] = i
        return proto_block_id_to_block_id

if __name__ == "__main__":
    assert len(sys.argv) == 3, "Please provide 1) the experiment directory, 2) the level of k"
    assert os.path.exists(sys.argv[1]), "The first argument should be a valid path to a directory"
    assert sys.argv[2].isdigit(), "The second argument should be an integer"

    experiment_directory = sys.argv[1]
    k = int(sys.argv[2])
    
    # Load the outcome from the condensed binary representation
    node_to_block, blocks = get_outcome(experiment_directory, k)

    # Load the summary graph from the uncondensed binary representation
    proto_edge_index, edge_type = get_summary_graph(experiment_directory, k)



    #############################################
    # Print some statistics for sanity checking #
    #############################################

    s = len("Smallest node in blocks:")
    print("Statistics:")
    graph_stats_file = experiment_directory + "ad_hoc_results/graph_stats.json"
    with open(graph_stats_file, "r") as f:
        graph_stats = json.load(f)
        vertex_count = graph_stats["Vertex count"]
    print("Vertex count:".rjust(s, " "), vertex_count)
    print("Node2block length:".rjust(s, " "), len(node_to_block))
    print("Node2block min:".rjust(s, " "), min(node_to_block))
    print("Node2block max:".rjust(s, " "), max(node_to_block))
    print("Blocks length:".rjust(s, " "), len(blocks))
    print("Block count:".rjust(s, " "), len(blocks[0]) + len(blocks)-1)
    print("Singleton count:".rjust(s, " "), len(blocks[0]))
    list_of_nodes = []
    for block in blocks:
        list_of_nodes += list(block)
    print("Total nodes in blocks:".rjust(s, " "), len(list_of_nodes))
    print("Smallest node in blocks:".rjust(s, " "), min(list_of_nodes))
    print("Largest node in blocks:".rjust(s, " "), max(list_of_nodes))
    print("Edge index length:".rjust(s, " "), len(proto_edge_index[0]))
    print("Edge type length:".rjust(s, " "), len(edge_type))
    maximum_block_node = -vertex_count
    minimum_block_node = vertex_count-1
    for i in range(len(proto_edge_index[0])):
        edge = [proto_edge_index[0][i], proto_edge_index[1][i]]
        for entity in edge:
            maximum_block_node = max(maximum_block_node, entity)
            minimum_block_node = min(minimum_block_node, entity)
    print("Largest block node id:".rjust(s, " "), maximum_block_node)
    print("Smallest block node id:".rjust(s, " "), minimum_block_node)



    ########################################
    # Print some tests for sanity checking #
    ########################################

    # All these tests should return True, otherwise there is something wrong
    s = len("Block count == Blocks length - 1 + Singleton count:")
    print("\nTests:")
    print("Node2block length == Vertex count:".rjust(s, " "), len(node_to_block) == vertex_count)
    print("Total nodes in blocks == Vertex count:".rjust(s, " "), len(list_of_nodes) == vertex_count)
    print("Smallest node in blocks == 0:".rjust(s, " "), min(list_of_nodes) == 0)
    print("Largest node in blocks == Vertex count - 1:".rjust(s, " "), max(list_of_nodes) == vertex_count-1)
    print("Node2block min < 2:".rjust(s, " "), min(node_to_block) < 2)
    print("Node2block max <= Block count - Singleton count:".rjust(s, " "), max(node_to_block) <= len(blocks[0]) + len(blocks)-1 - len(blocks[0]))
    print("Block count == Blocks length - 1 + Singleton count:".rjust(s, " "), len(blocks[0]) + len(blocks)-1 == len(blocks)-1 + len(blocks[0]))
    print("Edge index length == Edge type length:".rjust(s, " "), len(proto_edge_index[0]) == len(edge_type))



    #######################################
    # A quick demo on how to use the code #
    #######################################
    
    print("\nStarting short demo")

    # Load in the original graph
    edge_index, edge_type = get_original_graph(experiment_directory)
    print("Loading graph successful")

    # Load the outcome from the condensed binary representation
    proto_node_to_block, _ = get_outcome(experiment_directory, k)
    print("Loading node and block mappings successful")

    # Load the summary graph from the uncondensed binary representation
    proto_summary_edge_index, summary_edge_type = get_summary_graph(experiment_directory, k)
    print("Loading summary graph successful")

    # Map the block ids to [0..block_count-1]
    # We do this since the original blocks can have negative ids, while pytorch expects them to index a tensor
    positive_summary_node_map = make_positive_block_node_map(proto_node_to_block)
    print("Successfully created a mapping from the block ids to new positive ids")

    # Make a node to block mapping with the new node ids
    node_to_block = [positive_summary_node_map[id] for id in node_to_block]
    print("Successfully created the node to block mapping")

    # Make an edge index with the new node ids
    summary_edge_index = [[positive_summary_node_map[id] for id in proto_summary_edge_index[0]], [positive_summary_node_map[id] for id in proto_summary_edge_index[1]]]
    print("Successfully created the summary edge index")

    # Define or load input node features
    in_feature_size = 20
    out_feature_size = 10
    summary_vertex_count = len(positive_summary_node_map)
    summary_node_features = torch.rand((summary_vertex_count,in_feature_size))
    print("Successfully created random node features")

    # Define an RGCN layer that we can use for both the summary and original graph
    relation_type_count = len(set(summary_edge_type))  # This number is the same for the summary graphs and the original graph
    rgcn_layer = rgcn_conv.RGCNConv(in_channels=in_feature_size, out_channels=out_feature_size, num_relations=relation_type_count)
    print("Successfully created an RGCN layer")

    # Pass our data through an RGCN layer
    summary_out = rgcn_layer(summary_node_features, torch.as_tensor(summary_edge_index), torch.as_tensor(summary_edge_type))
    print("Successfully passed the summary graph through the RGCN layer")

    # Index select the summary node features for the full original graph
    node_features = summary_vectors_to_original_vectors(node_to_block, summary_node_features)
    print("Successfully created node features from summary node features")

    # We can reuse the same RGCN layer for the full graph
    original_out = rgcn_layer(node_features, torch.as_tensor(edge_index), torch.as_tensor(edge_type))
    print("Successfully passed the full graph through the same RGCN layer")