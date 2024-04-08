// #include <cstdint>
// #include <vector>
// #include <stack>
// #include <span>
// #include <fstream>
// #include <string>
// #include <boost/algorithm/string.hpp>
// #include <boost/dynamic_bitset.hpp>
// #include <boost/unordered/unordered_flat_map.hpp>
// #include <boost/unordered/unordered_flat_set.hpp>
// #include <boost/format.hpp>
// #include <iostream>
// #define BOOST_CHRONO_HEADER_ONLY
// #include <boost/chrono.hpp>
// #include <chrono>
// #include <iomanip>
// #include <thread>
// #include <boost/program_options.hpp>
// #include <boost/algorithm/string/find.hpp>

package bisimulation

import (
	"bufio"
	"encoding/binary"
	"hash/fnv"
	"io"
	"log"
	"os"
	"sort"

	"github.com/bits-and-blooms/bitset"
)

var logger = log.New(os.Stdout, "", log.Ldate|log.Ltime)

func main() {
	const is64Bit = uint64(^uintptr(0)) == ^uint64(0)
	if !is64Bit {
		panic("This code only works on 64 bit architectures because we need arrays larger than 2^32 size")
	}
}

type edgeType = uint32
type nodeIndex = uint64 // is assumed to be uint64, see check above
type blockIndex = nodeIndex

const BYTES_PER_ENTITY = 5
const BYTES_PER_PREDICATE = 4

type Edge struct {
	label  edgeType
	target nodeIndex
}

type Node struct {
	edges []Edge
}

func NewNode() *Node {
	edges := make([]Edge, 0, 1)
	return &Node{edges: edges}
}

func (n *Node) AddEdge(label edgeType, target nodeIndex) {
	n.edges = append(n.edges, Edge{label: label, target: target})
}

type Graph struct {
	nodes []Node
}

func NewGraph(expected_size int64) *Graph {
	nodes := make([]Node, 0, expected_size)
	return &Graph{nodes: nodes}

}

func (g *Graph) AddVertex() nodeIndex {
	g.nodes = append(g.nodes, *NewNode())
	return uint64(len(g.nodes) - 1)
}

// Make sure the underlying storage is ready for n more nodes
func (g *Graph) Reserve(n nodeIndex) {
	n_as_int := int(n)
	if cap(g.nodes)-len(g.nodes) < n_as_int {
		g.nodes = append(make([]Node, 0, len(g.nodes)+n_as_int), g.nodes...)
	}
}

// Add nodes to the graph until there are n nodes
func (g *Graph) Resize(n nodeIndex) {
	g.Reserve(n)
	for g.GetSize() < n {
		g.AddVertex()
	}
}

func (g *Graph) GetSize() nodeIndex {
	return uint64(len(g.nodes))
}

// class Graph
// {
// #ifdef CREATE_REVERSE_INDEX
//     std::vector<std::vector<node_index>> reverse;
//     void compute_reverse_index()
//     {
//         if (!this->reverse.empty())
//         {
//             throw MyException("computing the reverse while this has been computed before. Probably a programming error");
//         }
//         size_t number_of_nodes = this->nodes.size();
//         // we create it first with sets to remove duplicates
//         std::vector<boost::unordered_flat_set<node_index>> unique_index(number_of_nodes);
//         for (node_index sourceID = 0; sourceID < number_of_nodes; sourceID++)
//         {
//             Node &node = this->nodes[sourceID];
//             for (const Edge edge : node.get_outgoing_edges())
//             {
//                 node_index targetID = edge.target;
//                 unique_index[targetID].insert(sourceID);
//             }
//         }
//         // now convert to the final index
//         this->reverse.resize(number_of_nodes);
//         for (node_index targetID = 0; targetID < number_of_nodes; targetID++)
//         {
//             for (const node_index &sourceID : unique_index[targetID])
//             {
//                 this->reverse[targetID].push_back(sourceID);
//             }
//             // minimize memory usage
//             this->reverse[targetID].shrink_to_fit();
//         }
//     }
// #endif
// };

func readUintENTITYLittleEndian(r io.Reader) (nodeIndex, error) {
	var data [BYTES_PER_ENTITY]byte
	_, err := io.ReadFull(r, data[:])
	if err != nil {
		return 0, err
	}

	result := nodeIndex(0)
	for i := 0; i < BYTES_PER_ENTITY; i++ {
		result |= nodeIndex(data[i]) << (8 * i) // no need to mask the byte, this is not C++
	}

	return result, nil
}

func readUintPREDICATELittleEndian(r io.Reader) (edgeType, error) {
	var data [BYTES_PER_PREDICATE]byte
	_, err := io.ReadFull(r, data[:])
	if err != nil {
		return 0, err
	}

	result := edgeType(0)
	for i := 0; i < BYTES_PER_PREDICATE; i++ {
		result |= edgeType(data[i]) << (8 * i) // no need to mask the byte, this is not C++
	}

	return result, nil
}

func (g *Graph) ReadFromStream(r io.Reader) error {

	line_counter := uint64(0)

	var read_error error = nil
	for read_error == nil {
		line_counter++
		subject_index, read_error := readUintENTITYLittleEndian(r)
		if read_error != nil {
			break
		}
		edge_label, read_error := readUintPREDICATELittleEndian(r)
		if read_error != nil {
			break
		}
		object_index, read_error := readUintENTITYLittleEndian(r)
		if read_error != nil {
			break
		}
		var largest nodeIndex = 0
		if subject_index > object_index {
			largest = subject_index
		} else {
			largest = object_index
		}
		g.Resize(largest + 1)

		g.nodes[subject_index].AddEdge(edge_label, object_index)
		if line_counter%1000000 == 0 {
			logger.Printf("Done with %d triples\n", line_counter)
		}
	}
	if read_error != io.EOF {
		return read_error
	}
	// #ifdef CREATE_REVERSE_INDEX
	//     g.compute_reverse_index();
	// #endif
	return nil
}

func (g *Graph) ReadGraph(file_name string) error {
	f, err := os.Open(file_name)
	defer func() {
		f.Close()
	}()

	if err != nil {
		return err
	}
	const BufferSize = 8 * 16184
	reader := bufio.NewReaderSize(f, BufferSize)

	err = g.ReadFromStream(reader)
	return err
}

type Block []nodeIndex
type BlockPtr *Block

type DirtyBlockContainer interface {
	Clear()
	SetDirty(blockIndex)
	//GetFree() block_index
}

type SimpleDirtyBlockContainer struct {
	bits bitset.BitSet
}

func NewSimpleDirtyBlockContainer(size_hint uint64) *SimpleDirtyBlockContainer {
	return &SimpleDirtyBlockContainer{
		bits: *bitset.New(uint(size_hint)),
	}
}

func (d *SimpleDirtyBlockContainer) Clear() {
	d.bits.ClearAll()
	d.bits.Compact()
}

func (d *SimpleDirtyBlockContainer) SetDirty(index blockIndex) {

	d.bits.Set(uint(index))
}

// func (d *SimpleDirtyBlockContainer) GetFree() block_index {
// }

type Node2BlockMapper interface {
	/**
	 * get_block returns a positive numebr only in case the block really exists.
	 * If it is a singleton, a (singleton specific) negative number will be returned.
	 */
	GetBlock(nodeIndex) blockIndex
	Clear()
	SingletonCount() uint64
	ModifiableCopy() MappingNode2BlockMapper
	FreeBlockCount() uint64
}

type AllToZeroNode2BlockMapper struct {
	maxNodeIndex nodeIndex
}

func NewAllToZeroNode2BlockMapper(maxNodeIndex nodeIndex) *AllToZeroNode2BlockMapper {
	if maxNodeIndex < 2 {
		logger.Panicf("the graph has only one, or zero nodes, breaking the precondition for using the AllToZeroNode2BlockMapper. It assumes there will not be any singletons")
	}
	return &AllToZeroNode2BlockMapper{maxNodeIndex: maxNodeIndex}
}

func (mapper *AllToZeroNode2BlockMapper) GetBlock(nIndex nodeIndex) blockIndex {
	if nIndex >= mapper.maxNodeIndex {
		logger.Panicf("requested an index higher than the maxNodeIndex %d", mapper.maxNodeIndex)
	}
	return 0
}

func (mapper *AllToZeroNode2BlockMapper) Clear() {
	// Do nothing.
}

func (mapper *AllToZeroNode2BlockMapper) ModifiableCopy() MappingNode2BlockMapper {
	node_to_block := make([]nodeIndex, mapper.maxNodeIndex) // filled with zeroes by default
	empty_dirty := []uint64{}

	return *NewMappingNode2BlockMapper(node_to_block, 0, empty_dirty)
}

func (mapper *AllToZeroNode2BlockMapper) SingletonCount() nodeIndex {
	return 0
}

func (mapper *AllToZeroNode2BlockMapper) FreeBlockCount() uint64 {
	return 0
}

type MappingNode2BlockMapper struct {
	nodeToBlock      []blockIndex
	singletonCounter uint64
	freeBlockIndices []blockIndex // Assuming stack behavior is required, could implement using slice
}

// NewMappingNode2BlockMapper creates a new instance of MappingNode2BlockMapper.
// Assumes freeBlockIndices is a slice for simplicity.
func NewMappingNode2BlockMapper(nodeToBlock []blockIndex, singletonCount uint64, freeBlockIndices []blockIndex) *MappingNode2BlockMapper {
	return &MappingNode2BlockMapper{
		nodeToBlock:      nodeToBlock,
		singletonCounter: singletonCount,
		freeBlockIndices: freeBlockIndices,
	}
}

// FreeBlockCount implements the Node2BlockMapper interface for MappingNode2BlockMapper.
func (m *MappingNode2BlockMapper) FreeBlockCount() int {
	return len(m.freeBlockIndices)
}

// SingletonCount implements the Node2BlockMapper interface for MappingNode2BlockMapper.
func (m *MappingNode2BlockMapper) SingletonCount() uint64 {
	return m.singletonCounter
}

// OverwriteMapping updates the block mapping for a given node index.
func (m *MappingNode2BlockMapper) OverwriteMapping(nIndex nodeIndex, bIndex blockIndex) {
	m.nodeToBlock[nIndex] = bIndex
}

// GetBlock implements the Node2BlockMapper interface for MappingNode2BlockMapper.
func (m *MappingNode2BlockMapper) GetBlock(nIndex nodeIndex) blockIndex {
	return m.nodeToBlock[nIndex]
}

// Clear implements the Node2BlockMapper interface for MappingNode2BlockMapper.
func (m *MappingNode2BlockMapper) Clear() {
	m.nodeToBlock = []blockIndex{}
	m.singletonCounter = 0
	m.freeBlockIndices = []blockIndex{}
}

// ModifiableCopy implements the Node2BlockMapper interface for MappingNode2BlockMapper.
// This creates a deep copy of the mapper.
func (m *MappingNode2BlockMapper) ModifiableCopy() *MappingNode2BlockMapper {
	nodeToBlockCopy := make([]blockIndex, len(m.nodeToBlock))
	copy(nodeToBlockCopy, m.nodeToBlock)

	freeBlockIndicesCopy := make([]blockIndex, len(m.freeBlockIndices))
	copy(freeBlockIndicesCopy, m.freeBlockIndices)

	return &MappingNode2BlockMapper{
		nodeToBlock:      nodeToBlockCopy,
		singletonCounter: m.singletonCounter,
		freeBlockIndices: freeBlockIndicesCopy,
	}
}

// PutIntoSingleton marks a given node as a singleton.
func (m *MappingNode2BlockMapper) PutIntoSingleton(node nodeIndex) {
	// if m.nodeToBlock[node] < 0 {
	// 	logger.Panicf("Tried to create a singleton from node %d which already was a singleton. This is nearly certainly a mistake in the code.", node)
	// }
	m.singletonCounter++
	m.nodeToBlock[node] = -blockIndex(m.singletonCounter) // Negative value as per requirement.
}

type KBisimulationOutcome struct {
	Blocks      []BlockPtr
	DirtyBlocks DirtyBlockContainer
	// If the block for the node is not a singleton, this contains the block index.
	// Otherwise, this will contain a negative number unique for that singleton
	NodeToBlock Node2BlockMapper
}

// NewKBisimulationOutcome is a constructor for KBisimulationOutcome in Go.
func NewKBisimulationOutcome(blocks []BlockPtr, dirtyBlocks DirtyBlockContainer, nodeToBlock Node2BlockMapper) *KBisimulationOutcome {
	return &KBisimulationOutcome{
		Blocks:      blocks,
		DirtyBlocks: dirtyBlocks,
		NodeToBlock: nodeToBlock,
	}
}

// ClearIndices clears the indices in NodeToBlock and DirtyBlocks.
func (kbo *KBisimulationOutcome) ClearIndices() {
	kbo.NodeToBlock.Clear()
	kbo.DirtyBlocks.Clear()
}

// GetBlockIDForNode returns the block ID for a given node.
func (kbo *KBisimulationOutcome) GetBlockIDForNode(node nodeIndex) uint64 {
	return kbo.NodeToBlock.GetBlock(node)
}

// SingletonBlockCount returns the count of singleton blocks.
func (kbo *KBisimulationOutcome) SingletonBlockCount() uint64 {
	return kbo.NodeToBlock.SingletonCount()
}

// NonSingletonBlockCount returns the count of non-singleton blocks.
func (kbo *KBisimulationOutcome) NonSingletonBlockCount() uint64 {
	blocksAllocated := uint64(len(kbo.Blocks))
	unusedBlocks := kbo.NodeToBlock.FreeBlockCount()
	return blocksAllocated - unusedBlocks
}

// TotalBlocks calculates the total number of blocks.
func (kbo *KBisimulationOutcome) TotalBlocks() uint64 {
	return kbo.SingletonBlockCount() + kbo.NonSingletonBlockCount()
}

// Get0Bisimulation computes 0-bisimulation for a given graph.
func Get0Bisimulation(g *Graph) *KBisimulationOutcome {
	amount := g.GetSize()
	block := make(Block, 0, amount)

	for i := uint64(0); i < amount; i++ {
		block = append(block, i)
	}
	var newBlocks []BlockPtr
	newBlocks = append(newBlocks, &block)

	nodeToBlock := NewAllToZeroNode2BlockMapper(amount)

	dirty := NewSimpleDirtyBlockContainer(1)
	dirty.SetDirty(0)

	result := NewKBisimulationOutcome(newBlocks, dirty, nodeToBlock)
	return result
}

type SignaturePiece struct {
	label edgeType
	block blockIndex
}

type Signature struct {
	pieces []SignaturePiece
}

func NewSignature(sorted_pieces []SignaturePiece) *Signature {
	return &Signature{pieces: sorted_pieces}
}

func (s *Signature) Hash() uint64 {
	// We compute a hash of the pieces by adding them all. Since they are assumed sorted, this is correct
	hash := fnv.New64a()
	b := make([]byte, 8)
	for _, piece := range s.pieces {
		binary.LittleEndian.PutUint32(b[0:4], piece.label)
		hash.Write(b[0:4])
		binary.LittleEndian.PutUint64(b, piece.block)
		hash.Write(b)
	}
	return hash.Sum64()
}

type SignatureBuilder struct {
	unsorted_pieces []SignaturePiece
	deduplicate     bool
}

func NewSignatureBuilder(deduplicate bool) SignatureBuilder {
	return SignatureBuilder{
		unsorted_pieces: make([]SignaturePiece, 0, 10),
		deduplicate:     deduplicate,
	}
}

func (b *SignatureBuilder) AddPiece(label edgeType, block blockIndex) {

	if b.deduplicate {
		// we do a quick dedup so we avoid sorting them later. It is expected that reasonably often duplicates are in order already
		if b.unsorted_pieces[len(b.unsorted_pieces)-1].block == block && b.unsorted_pieces[len(b.unsorted_pieces)-1].label == label {
			// nothing to be added
			return
		}
	}

	b.unsorted_pieces = append(b.unsorted_pieces, SignaturePiece{label, block})
}

func (b *SignatureBuilder) Build() *Signature {
	// minimize the space used by the signature

	if len(b.unsorted_pieces) == 0 {
		log.Panicln("The number of pieces for this signature is 0, which is not possible.")
	}

	var dst []SignaturePiece
	dst = make([]SignaturePiece, len(b.unsorted_pieces))
	copy(dst, b.unsorted_pieces)

	sort.Slice(dst, func(i, j int) bool {
		if dst[i].label < dst[j].label {
			return true
		}
		return dst[i].block < dst[j].block
	})

	if b.deduplicate {
		// deduplicate in place
		current_last := 0
		for i := 1; i < len(dst); i++ {
			if dst[i] == dst[current_last] {
				continue
			}
			// it is not a duplicate
			dst[current_last+1] = dst[i]
			current_last += 1
		}

		dst_truncated := make([]SignaturePiece, current_last+1)
		copy(dst_truncated, dst[:current_last+1])
		dst = dst_truncated
	}

	return NewSignature(dst)
}

type SignatureBlockMap struct {
	mapping map[uint64][]struct {
		s     *Signature
		block Block
	}
}

func NewSignatureBlockMap() SignatureBlockMap {
	m := make(map[uint64][]struct {
		s     *Signature
		block Block
	})
	return SignatureBlockMap{
		mapping: m,
	}
}

func (M *SignatureBlockMap) Put(new_signature *Signature, index nodeIndex) {
	hash := new_signature.Hash()
	possible_options := M.mapping[hash]
OptionLoop:
	for _, option := range possible_options {
		// only if the signature is equal, this option is a real match
		if len(option.s.pieces) != len(new_signature.pieces) {
			continue
		}
		// The pieces in a signature are sorted, we use that here.
		for i := 0; i < len(option.s.pieces); i++ {
			if option.s.pieces[i] != new_signature.pieces[i] {
				continue OptionLoop
			}
		}
		// all compared equal, we have a match!
		option.block = append(option.block, index)
		return
	}
	// not found, add a new one
	newEntry := struct {
		s     *Signature
		block Block
	}{
		s:     new_signature,
		block: make([]uint64, 1, 1),
	}
	newEntry.block[0] = index

	newEntryList := make([]struct {
		s     *Signature
		block Block
	}, 1, 1)
	newEntryList[0] = newEntry

	M.mapping[hash] = newEntryList
}

func (M *SignatureBlockMap) SizeIsOne() bool {
	mappings := len(M.mapping)
	if mappings > 1 || mappings == 0 {
		return false
	}
	// there is only one mapping, but there could be multiple entries in there
	for _, bucket := range M.mapping {
		return len(bucket) == 1
	}
	logger.Panic("SizeOfOne got in an impossible to reach point o the code")
	return false
}

func (M *SignatureBlockMap) GetBlocks() []Block {
	blocks := make([]Block, 0, len(M.mapping)) //usually there won't be any collisions...
	for _, bucket := range M.mapping {
		for _, item := range bucket {
			blocks = append(blocks, item.block)
		}
	}
	return blocks
}

// Because our signatures are complex, we do a custom hash first and will then iterate over potential candidates

func PartialKBisimulation(g *Graph, kBlock []BlockPtr, kMinOneMapper Node2BlockMapper, myDirtyBlocks []blockIndex, freeBlocksInput []blockIndex, minSupport uint64) {

	//TODO We might be able to optimize for blocks of size two first as was done in the C++ version,

	myFreeBlocks := make([]blockIndex, 0, len(freeBlocksInput)+1000)
	copy(myFreeBlocks, freeBlocksInput)
	freeBlocksInput = nil // prevent accidental modifications

	nodes_from_split_blocks := make([]nodeIndex, 0)

	newSingletons := make([]nodeIndex, 0)

	// TODO we can quite easily parallelize this better. Do the computation of Ms in man go routines
	for _, dirtyBlockIndex := range myDirtyBlocks {
		dirtyBlock := kBlock[dirtyBlockIndex]
		dirtyBlockSize := uint64(len(*dirtyBlock))
		if dirtyBlockSize < minSupport {
			continue
		}
		M := NewSignatureBlockMap()
		for _, nodeInDirtyBlock := range *dirtyBlock {
			signatureBuilder := NewSignatureBuilder(true)

			for _, edge_info := range g.nodes[nodeInDirtyBlock].edges {
				to_block := kMinOneMapper.GetBlock(edge_info.target)
				signatureBuilder.AddPiece(edge_info.label, to_block)
			}
			signature := signatureBuilder.Build()
			M.Put(signature, nodeInDirtyBlock)
		}
		if M.SizeIsOne() {
			// no need to update anything in the blocks, nor in the index
			continue
		}
		// add all nodes in the block to be split to the nodes_from_split_blocks
		nodes_from_split_blocks = append(nodes_from_split_blocks, *dirtyBlock...)
		// We mark the current block_index as aa free one, and set it to an empty one (nil)
		myFreeBlocks = append(myFreeBlocks, dirtyBlockIndex)
		kBlock[dirtyBlockIndex] = nil

		// all indices for nodes in this block will be overwritten, so no need to do this now
		// categorize the blocks
		for _, newBlock := range M.GetBlocks() {
			// if singleton, make it a singleton in the mapping
			if len(newBlock) == 1 {
				newSingletons = append(newSingletons, newBlock[0])
				continue
			}
			// else
			// if there are still known empty blocks, write on them
			var newBlockIndex blockIndex
			if len(myFreeBlocks) > 0 {
				newBlockIndex = myFreeBlocks[len(myFreeBlocks)-1]
				myFreeBlocks = myFreeBlocks[:len(myFreeBlocks)-1]
				kBlock[newBlockIndex] = &newBlock
			} else {
				newBlockIndex = uint64(len(kBlock))
				kBlock = append(kBlock, &newBlock)
			}
			// we need to update the nodeToBlockIndex, except for the block which would get put back onto its parent block's location
			if newBlockIndex != dirtyBlockIndex {

			}
		}
	}

	return newSingletons, // new nodes which have become singletons
		freeBlocks, // blocks in kBlock freed by this function
		newBlocks, // The mapping to these blocks still needs to be written
		dirtyBlocks // blocks marked as dirty by this function. Needs merging with the ones from parallel calls.
}

// func GetKBisimulation(g *Graph, kMinusOneOutcome *KBisimulationOutcome, minSupport int) *KBisimulationOutcome {
// 	// Make copies which we will modify.
// 	kBlocks := make([]BlockPtr, len(kMinusOneOutcome.Blocks))
// 	copy(kBlocks, kMinusOneOutcome.Blocks)

// 	kNodeToBlock := kMinusOneOutcome.NodeToBlock.ModifiableCopy()

// 	// We collect all nodes from split blocks to mark all blocks which target these as dirty (at the very end).
// 	nodesFromSplitBlocks := make(map[nodeIndex]struct{})

// 	// we first do dirty blocks of size 2 because if they split, they cause two singletons and a gap (freeblock) in the list of blocks
// 	// These freeblocks can be filled if larger blocks are split.

// 	if (min_support < 2){
// 	for _, dirtyBlockIndex := range kMinusOneOutcome.DirtyBlocks.List() { // Assuming DirtyBlockContainer has a method List() that returns the indices of dirty blocks.
// 		dirtyBlock := kBlocks[dirtyBlockIndex]
// 		dirtyBlockSize := len(dirtyBlock.Nodes) // Assuming Block has a field Nodes which is a slice of node indices.

// 		if dirtyBlockSize == 2 || dirtyBlockSize <= minSupport {
// 			continue // If it's already processed or too small, skip it.
// 		}

// 		// Process block splitting logic here...
// 		// This example does not implement the detailed logic of block splitting and signature collection due to complexity.

// 		// Mark blocks as dirty or split them as needed.
// 	}

// 	// Update dirty blocks based on splits...
// 	// This section requires detailed implementation based on the graph's structure and the splitting logic.

// 	outcome := NewKBisimulationOutcome(kBlocks, dirty, kNodeToBlock)
// 	return outcome, nil
// }
