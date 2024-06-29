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
	"sync"

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
type blockOrSingletonIndex = int64

const BYTES_PER_ENTITY = 5
const BYTES_PER_PREDICATE = 4
const MAX_INT = uint64(^uint64(0) >> 1)

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

// Our custom implementation for sets used for calculating the reverse index
type NodeHashSet struct {
	members map[nodeIndex]bool
}

func NewNodeHashSet() *NodeHashSet {
	members := make(map[nodeIndex]bool)
	return &NodeHashSet{members: members}
}

func (hs *NodeHashSet) Add(node nodeIndex) {
	hs.members[node] = true
}

func (hs *NodeHashSet) ToSlice() []nodeIndex {
	keys := make([]nodeIndex, len(hs.members))
	i := 0
	for k := range hs.members {
		keys[i] = k
		i++
	}
	return keys
}

// Our custom implementation for sets used for marking dirty blocks
type BlockHashSet struct {
	members map[blockIndex]bool
}

func NewBlockHashSet() *BlockHashSet {
	members := make(map[blockIndex]bool)
	return &BlockHashSet{members: members}
}

func (hs *BlockHashSet) Add(block blockIndex) {
	hs.members[block] = true
}

func (hs *BlockHashSet) ToSlice() []blockIndex {
	keys := make([]blockIndex, len(hs.members))
	i := 0
	for k := range hs.members {
		keys[i] = k
		i++
	}
	return keys
}

type Graph struct {
	nodes   []Node
	reverse map[nodeIndex][]nodeIndex
}

func NewGraph(expected_size int64) *Graph {
	nodes := make([]Node, 0, expected_size)
	reverse := make(map[nodeIndex][]nodeIndex, expected_size)
	return &Graph{nodes: nodes, reverse: reverse}

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

func (g *Graph) GetParents(childNode nodeIndex) []nodeIndex {
	return g.reverse[childNode]
}

func (g *Graph) CreateReverseIndex() {
	if len(g.reverse) != 0 {
		panic("computing the reverse while this has been computed before. Probably a programming error")
	}
	numberOfNodes := g.GetSize()
	uniqueIndex := make(map[nodeIndex]*NodeHashSet, numberOfNodes)

	var i uint64
	for i = 0; i < numberOfNodes; i++ {
		uniqueIndex[i] = NewNodeHashSet()
	}

	var sourceID nodeIndex
	var targetID nodeIndex

	for sourceID = 0; sourceID < numberOfNodes; sourceID++ {
		node := g.nodes[sourceID]
		for _, edge := range node.edges {
			targetID := edge.target
			uniqueIndex[targetID].Add(sourceID)
		}
	}

	for targetID = 0; targetID < numberOfNodes; targetID++ {
		for sourceID := range uniqueIndex[targetID].members {
			g.reverse[targetID] = append(g.reverse[targetID], sourceID)
		}
	}
	logger.Println("Done creating reverse index")
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
	for {
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

		largest := max(subject_index, object_index)
		g.Resize(largest + 1)

		g.nodes[subject_index].AddEdge(edge_label, object_index)
		if line_counter%1000000 == 0 {
			logger.Printf("Done with %d triples\n", line_counter)
		}
	}
	if read_error != io.EOF && read_error != nil {
		logger.Printf("DEBUG Lil error: %v", read_error)
		return read_error
	}
	logger.Println("Done reading graph")
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
	 * get_block returns a positive number only in case the block really exists.
	 * If it is a singleton, a (singleton specific) negative number will be returned.
	 */
	GetBlock(nodeIndex) blockOrSingletonIndex // Previously blockIndex, but we need negative numbers to indicate singletons
	Clear()
	SingletonCount() uint64
	ModifiableCopy() *MappingNode2BlockMapper
	FreeBlockCount() uint64
}

type AllToZeroNode2BlockMapper struct {
	graphNodeCount nodeIndex
}

func NewAllToZeroNode2BlockMapper(graphNodeCount nodeIndex) *AllToZeroNode2BlockMapper {
	if graphNodeCount < 2 {
		logger.Panicf("the graph has only one, or zero nodes, breaking the precondition for using the AllToZeroNode2BlockMapper. It assumes there will not be any singletons")
	}
	return &AllToZeroNode2BlockMapper{graphNodeCount: graphNodeCount}
}

func (mapper *AllToZeroNode2BlockMapper) GetBlock(nIndex nodeIndex) blockOrSingletonIndex {
	if nIndex > uint64(MAX_INT) {
		panic("Tried to get a block index lager than MAX_INT (maximum int64 value)")
	}
	if nIndex >= mapper.graphNodeCount {
		logger.Panicf("requested an index higher than or equal to the graphNodeCount %d", mapper.graphNodeCount)
	}
	return 0
}

func (mapper *AllToZeroNode2BlockMapper) Clear() {
	// Do nothing.
}

func (mapper *AllToZeroNode2BlockMapper) ModifiableCopy() *MappingNode2BlockMapper {
	node_to_block := make([]blockOrSingletonIndex, mapper.graphNodeCount) // filled with zeroes by default
	empty_dirty := []uint64{}

	return NewMappingNode2BlockMapper(node_to_block, 0, empty_dirty)
}

func (mapper *AllToZeroNode2BlockMapper) SingletonCount() nodeIndex {
	return 0
}

func (mapper *AllToZeroNode2BlockMapper) FreeBlockCount() uint64 {
	return 0
}

type MappingNode2BlockMapper struct {
	nodeToBlock      []blockOrSingletonIndex
	singletonCounter uint64
	freeBlockIndices []blockIndex // Assuming stack behavior is required, could implement using slice
}

// NewMappingNode2BlockMapper creates a new instance of MappingNode2BlockMapper.
// Assumes freeBlockIndices is a slice for simplicity.
func NewMappingNode2BlockMapper(nodeToBlock []blockOrSingletonIndex, singletonCount uint64, freeBlockIndices []blockIndex) *MappingNode2BlockMapper {
	return &MappingNode2BlockMapper{
		nodeToBlock:      nodeToBlock,
		singletonCounter: singletonCount,
		freeBlockIndices: freeBlockIndices,
	}
}

// FreeBlockCount implements the Node2BlockMapper interface for MappingNode2BlockMapper.
func (m *MappingNode2BlockMapper) FreeBlockCount() uint64 {
	return uint64(len(m.freeBlockIndices))
}

// SingletonCount implements the Node2BlockMapper interface for MappingNode2BlockMapper.
func (m *MappingNode2BlockMapper) SingletonCount() uint64 {
	return m.singletonCounter
}

// OverwriteMapping updates the block mapping for a given node index.
func (m *MappingNode2BlockMapper) OverwriteMapping(nIndex nodeIndex, bIndex blockOrSingletonIndex) {
	m.nodeToBlock[nIndex] = bIndex
}

// GetBlock implements the Node2BlockMapper interface for MappingNode2BlockMapper.
func (m *MappingNode2BlockMapper) GetBlock(nIndex nodeIndex) blockOrSingletonIndex {
	if nIndex > uint64(MAX_INT) {
		panic("Tried to get a block index lager than MAX_INT (maximum int64 value)")
	}
	return blockOrSingletonIndex(m.nodeToBlock[nIndex])
}

// Clear implements the Node2BlockMapper interface for MappingNode2BlockMapper.
func (m *MappingNode2BlockMapper) Clear() {
	m.nodeToBlock = []blockOrSingletonIndex{}
	m.singletonCounter = 0
	m.freeBlockIndices = []blockIndex{}
}

// ModifiableCopy implements the Node2BlockMapper interface for MappingNode2BlockMapper.
// This creates a deep copy of the mapper.
func (m *MappingNode2BlockMapper) ModifiableCopy() *MappingNode2BlockMapper {
	nodeToBlockCopy := make([]blockOrSingletonIndex, len(m.nodeToBlock))
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
	m.nodeToBlock[node] = -blockOrSingletonIndex(m.singletonCounter) // Negative value as per requirement.
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
func (kbo *KBisimulationOutcome) GetBlockIDForNode(node nodeIndex) blockOrSingletonIndex {
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

type FBLSChannels struct {
	freeBlocksChannel  chan blockIndex
	blocksChannel      chan []BlockPtr
	largeBlocksChannel chan BlockPtr
	singletonsChannel  chan BlockPtr
}

func NewFBLSChannels(freeBlocksBufferSize int, blocksChannelBufferSize int, largeBlocksChannelBufferSize int, singletonsChannelBufferSize int) FBLSChannels {
	freeBlocksChannel := make(chan blockIndex, freeBlocksBufferSize)
	blocksChannel := make(chan []BlockPtr, blocksChannelBufferSize)
	largeBlocksChannel := make(chan BlockPtr, largeBlocksChannelBufferSize)
	singletonsChannel := make(chan BlockPtr, singletonsChannelBufferSize)
	return FBLSChannels{freeBlocksChannel: freeBlocksChannel, blocksChannel: blocksChannel, largeBlocksChannel: largeBlocksChannel, singletonsChannel: singletonsChannel}
}

type ConcurrentKBisimulationOutcome struct {
	Blocks      []BlockPtr
	DirtyBlocks []blockIndex
	// If the block for the node is not a singleton, NodeToBlock contains the block index.
	// Otherwise, it will contain a negative number unique for that singleton
	NodeToBlock    Node2BlockMapper
	freeBlocks     []blockIndex
	channels       FBLSChannels
	singletonCount uint64
}

func NewConcurrentKBisimulationOutcome(blocks []BlockPtr, dirtyBlocks []nodeIndex, nodeToBlock Node2BlockMapper, freeBlocks []blockIndex, channels FBLSChannels, singletonCount uint64) *ConcurrentKBisimulationOutcome {
	return &ConcurrentKBisimulationOutcome{
		Blocks:         blocks,
		DirtyBlocks:    dirtyBlocks,
		NodeToBlock:    nodeToBlock,
		freeBlocks:     freeBlocks,
		channels:       channels,
		singletonCount: singletonCount,
	}
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
	block blockOrSingletonIndex
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

		// Convert potential negative indices into positive ones
		// TODO check if this is valid
		var block uint64
		if piece.block >= 0 {
			block = uint64(piece.block)
		} else {
			block = MAX_INT + uint64(-piece.block)
		}

		binary.LittleEndian.PutUint64(b, block)
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

func (b *SignatureBuilder) AddPiece(label edgeType, block blockOrSingletonIndex) {

	if b.deduplicate {
		// we do a quick dedup so we avoid sorting them later. It is expected that reasonably often duplicates are in order already
		if len(b.unsorted_pieces) > 0 && b.unsorted_pieces[len(b.unsorted_pieces)-1].block == block && b.unsorted_pieces[len(b.unsorted_pieces)-1].label == label {
			// nothing to be added
			return
		}
	}

	b.unsorted_pieces = append(b.unsorted_pieces, SignaturePiece{label, block})
}

func (b *SignatureBuilder) Build() *Signature {
	// minimize the space used by the signature

	// if len(b.unsorted_pieces) == 0 {
	// 	log.Panicln("The number of pieces for this signature is 0, which is not possible.")
	// }

	if len(b.unsorted_pieces) == 0 {
		dst := make([]SignaturePiece, 0)
		return NewSignature(dst)
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
		dst = uniqSortedDestructive(dst)
	}

	return NewSignature(dst)
}

/*
Removes duplicates from the list, assuming it is sorted to start with.
The provided list cannot be used after this operation (might be modified, or reused as a return value)
*/
func uniqSortedDestructive[V interface {
	comparable
}](sortedPieces []V) []V {

	// deduplicate in place
	if len(sortedPieces) == 0 {
		return sortedPieces
	}
	current_last := 0
	for i := 1; i < len(sortedPieces); i++ {
		if sortedPieces[current_last] == sortedPieces[i] {
			continue
		}
		// it is not a duplicate
		current_last += 1
		sortedPieces[current_last] = sortedPieces[i] // note: we could check whether current_last == i, and avoid overwriting. But probably that test will have about the same cost as just going ahead.
	}
	if current_last+1 == len(sortedPieces) {
		return sortedPieces
	} else {
		dst_truncated := make([]V, current_last+1)
		copy(dst_truncated, sortedPieces[:current_last+1])
		return dst_truncated
	}
}

func (signature *Signature) Equals(other *Signature) bool {
	// only if the signature is equal, this option is a real match
	if len(signature.pieces) != len(other.pieces) {
		return false
	}
	// The pieces in a signature are sorted, we use that here.
	for i := 0; i < len(signature.pieces); i++ {
		if signature.pieces[i] != other.pieces[i] {
			return false
		}
	}
	return true
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
	for i := 0; i < len(possible_options); i++ {
		if possible_options[i].s.Equals(new_signature) {
			possible_options[i].block = append(possible_options[i].block, index)
			return
		}
	}

	// not found, add a new one
	newEntry := struct {
		s     *Signature
		block Block
	}{
		s:     new_signature,
		block: make([]uint64, 1),
	}
	newEntry.block[0] = index

	newEntryList := make([]struct {
		s     *Signature
		block Block
	}, 1)
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

/*
Merge the other_M into this_M. The otherM object will be modified, some parts will be borrowed and it must no longer be used
*/
func (thisM *SignatureBlockMap) MergeDestructive(otherM *SignatureBlockMap) {
	for otherSignatureHash, otherOptions := range otherM.mapping {

		thisOptions, ok := thisM.mapping[otherSignatureHash]
		if !ok {
			// Add as a fresh entry
			thisM.mapping[otherSignatureHash] = otherOptions
			continue
		}
		// We have an existing entry with the same signatureHash, the otherOptions need to be merged into thisOptions
		// We have to check all pairs, short cutting if found
	OtherOptionsLoop:
		for _, otherOption := range otherOptions {
			for i := 0; i < len(thisOptions); i++ { // Changed this to a regular for loop, because a for each loop makes copies, instead of referencing the original data
				if otherOption.s.Equals(thisOptions[i].s) {
					thisOptions[i].block = append(thisOptions[i].block, otherOption.block...)
					continue OtherOptionsLoop
				}
			}
			// We have not found a match with one of the existing options, add a new one
			thisM.mapping[otherSignatureHash] = append(thisM.mapping[otherSignatureHash], otherOption)
		}
	}
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

	// return newSingletons, // new nodes which have become singletons
	// 	newBlockIndex, // blocks in kBlock freed by this function
	// 	newBlocks, // The mapping to these blocks still needs to be written
	//		dirtyBlocks // blocks marked as dirty by this function. Needs merging with the ones from parallel calls.
}

const minChunkSize = 100

func processBlock(kBlock *[]BlockPtr, kMinOneMapper Node2BlockMapper, g *Graph, currentBlockIndex blockIndex, blocksChannel chan []BlockPtr, freeBlocksChannel chan blockIndex, singletonsChannel chan BlockPtr, largeBlocksChannel chan BlockPtr) {
	currentBlock := (*kBlock)[currentBlockIndex]

	blockSize := blockIndex(len(*currentBlock))

	chunkSize := min(minChunkSize, blockSize)

	chunkCount := uint64(blockSize / chunkSize) // We are working with only positive numbers, so we can just truncate
	// If there is a remainder, we will spawn an extra thread of the final smaller chunk
	if blockSize%chunkSize != 0 {
		chunkCount += 1
	}
	// fmt.Printf("DEBUG chunk count: %d\n", chunkCount)

	// We create the channel the inner threads will use to communicate with this thread
	signatureBufferSize := chunkCount
	signatures := make(chan SignatureBlockMap, signatureBufferSize)

	// Process all chunk of size chunkSize
	for i := uint64(0); i < chunkCount-1; i++ {
		go processChunk(uint64(i*chunkSize), uint64((i+1)*chunkSize), currentBlock, kMinOneMapper, signatures, g)
	}
	// Process the last chunk, which may be smaller than chunk_size if blockSize/chunkSize would leave a remainder
	go processChunk((chunkCount-1)*chunkSize, blockSize, currentBlock, kMinOneMapper, signatures, g)

	// Listen to the signatures channel for all messages. We can initialize this signature map with the first map read from the signatures channel
	M := <-signatures
	// fmt.Printf("DEBUG m: %v\n", M)
	for i := uint64(0); i < chunkCount-1; i++ {
		m := <-signatures
		// fmt.Printf("DEBUG m: %v\n", m)
		M.MergeDestructive(&m)
	}
	// fmt.Printf("DEBUG M: %v\n", M)

	// Accumulate the singletons before sending them all together
	newSingletons := make(Block, 0)

	// We will use these variables to keep track of the largest block
	var largestBlock BlockPtr
	largestSize := 0

	// Here we store pointers to the created blocks
	newBlocks := make([]BlockPtr, 0)

	// If there is only one key in the mapping, then all the nodes have the same signature, meaning the current block did not split
	if len(M.mapping) == 1 {
		singletonsChannel <- &newSingletons // This should be empty
		blocksChannel <- newBlocks          // This should be empty
		emptyBlock := make(Block, 0)
		largeBlocksChannel <- &emptyBlock // This should be empty
		return
	}

	// We go over all newly created blocks
	for _, newBlock := range M.GetBlocks() {
		newBlockOnStack := make(Block, 0)
		newBlockOnStack = append(newBlockOnStack, newBlock...) // We do this because every newBlock would actually shares the same adress (as opposed to each newBlockOnStack having a unique adress)
		// if singleton, make it a singleton in the mapping
		if len(newBlockOnStack) == 1 {
			newSingletons = append(newSingletons, newBlockOnStack[0])
			continue
		}

		// Keep track of the largest block, we ignore singletons because they will be written to a separate channel anyway
		if len(newBlockOnStack) > largestSize {
			// Write the old largest block directly if there is a free index, else store it in newBlocks
			if largestSize > 0 {
				select {
				case freeIndex := <-freeBlocksChannel:
					(*kBlock)[freeIndex] = largestBlock
				default:
					newBlocks = append(newBlocks, largestBlock)
				}
			}
			largestBlock = &newBlockOnStack
			largestSize = len(newBlockOnStack)
		} else {
			// Write the new block directly if there is a free index, else store it in newBlocks
			select {
			case freeIndex := <-freeBlocksChannel:
				(*kBlock)[freeIndex] = &newBlockOnStack
			default:
				newBlocks = append(newBlocks, &newBlockOnStack)
			}
		}
	}

	// Write the largest block directly into the current block (this is safe since the current thread is the only one accessing this block)
	// If no block larger than a singleton is found, then the current block must have split into only singletons, allowing us to reuse the block index
	if largestSize > 0 {
		(*kBlock)[currentBlockIndex] = largestBlock
		largeBlocksChannel <- (*kBlock)[currentBlockIndex]
	} else {
		emptyBlock := make(Block, 0)
		largeBlocksChannel <- &emptyBlock
		freeBlocksChannel <- currentBlockIndex
	}

	// Write the remaining blocks and singletons to the blocksChannel and singletonsChannel respectively
	singletonsChannel <- &newSingletons
	blocksChannel <- newBlocks
}

func processChunk(chunkStart uint64, chunkStop uint64, currentBlock BlockPtr, kMinOneMapper Node2BlockMapper, signatures chan SignatureBlockMap, g *Graph) {
	m := NewSignatureBlockMap()
	for _, node := range (*currentBlock)[chunkStart:chunkStop] {
		signatureBuilder := NewSignatureBuilder(true)
		for _, edge_info := range g.nodes[node].edges {
			to_block := kMinOneMapper.GetBlock(edge_info.target)
			signatureBuilder.AddPiece(edge_info.label, to_block)
		}
		signature := signatureBuilder.Build()
		m.Put(signature, node)
	}
	signatures <- m
}

func readSingletonsChannel(singletonsChannel chan BlockPtr, newSingletons *Block, kMapper *MappingNode2BlockMapper, blockCount int, singletonCount *uint64, wg *sync.WaitGroup) {
	for i := 0; i < blockCount; i++ {
		singletonBlock := <-singletonsChannel
		*newSingletons = append(*newSingletons, *singletonBlock...)
		for _, node := range *singletonBlock {
			*singletonCount++
			kMapper.OverwriteMapping(node, int64(-*singletonCount))
		}
	}
	(*wg).Done()
}

func readBlocksChannel(kBlock *[]BlockPtr, blocksChannel chan []BlockPtr, newBlocks *[]BlockPtr, freeBlocksChannel chan blockIndex, kMapper *MappingNode2BlockMapper, blockCount int, wg *sync.WaitGroup) {
	newIndex := blockCount
	for i := 0; i < blockCount; i++ {
		blockSlice := <-blocksChannel
		for _, block := range blockSlice {
			select {
			case freeIndex := <-freeBlocksChannel:
				(*kBlock)[freeIndex] = block
			default:
				*newBlocks = append(*newBlocks, block)
			}
			for _, node := range *block {
				kMapper.OverwriteMapping(node, (int64(newIndex)))
			}
			newIndex++
		}
	}
	(*wg).Done()
}

func readLargeBlocksChannel(largeBlocksChannel chan BlockPtr, changedBlocks *[]BlockPtr, blockCount int, wg *sync.WaitGroup) {
	for i := 0; i < blockCount; i++ {
		block := <-largeBlocksChannel
		*changedBlocks = append(*changedBlocks, block)
	}
	(*wg).Done()
}

func fillFreeBlocksChannel(freeBlocks *[]blockIndex, freeBlocksChannel chan blockIndex, wgFreeBlocks *sync.WaitGroup) {
	// Put the slice content into the channel
	for _, freeBlock := range *freeBlocks {
		freeBlocksChannel <- freeBlock
	}

	// Empty the slice
	*freeBlocks = make([]blockIndex, 0)
	wgFreeBlocks.Done()
}

// func updateKMapper(kMapper *MappingNode2BlockMapper, block BlockPtr, newIndex int) {
// 	for _, node := range *block {
// 		kMapper.OverwriteMapping(node, uint64(newIndex))
// 	}
// }

// func largestBlockMarkDirty(g *Graph, kBlock *[]BlockPtr, kMapper *MappingNode2BlockMapper, block blockIndex) {
// 	for _, node := range *block {
// 		kMapper.OverwriteMapping(node, uint64(newIndex))
// 	}
// }

func MultiThreadKBisimulationStepZero(g *Graph) *ConcurrentKBisimulationOutcome {
	// Put all nodes into one large block
	numberOfNodes := g.GetSize()
	blockData := make(Block, numberOfNodes)
	for i := range blockData {
		blockData[i] = uint64(i)
	}

	// Make the block index to node map
	expectedBlocksSize := 100
	blocks := make([]BlockPtr, 0, expectedBlocksSize)
	blocks = append(blocks, &blockData)

	// Make the slice for containing dirty block indices
	expectedDirtyBlocksSize := 100
	dirtyBlocks := make([]blockIndex, 0, expectedDirtyBlocksSize)
	dirtyBlocks = append(dirtyBlocks, 0)

	// Make the node to block mapper
	node2BlockMapper := NewAllToZeroNode2BlockMapper(numberOfNodes)

	// Make the free blocks buffer
	expectedFreeBlocksSize := 100
	freeBlocks := make([]blockIndex, 0, expectedFreeBlocksSize)

	// Setup the channels, which we will reuse for later steps
	freeBlocksBufferSize := 100
	blocksChannelBufferSize := 100
	largeBlocksChannelBufferSize := 100
	singletonsChannelBufferSize := 100
	channels := NewFBLSChannels(freeBlocksBufferSize, blocksChannelBufferSize, largeBlocksChannelBufferSize, singletonsChannelBufferSize)

	// Set the singleton count to 0. This assumes a graph with more than one node
	singletonCount := uint64(0)

	return NewConcurrentKBisimulationOutcome(blocks, dirtyBlocks, node2BlockMapper, freeBlocks, channels, singletonCount)
}

func MultiThreadKBisimulationStep(g *Graph, kMinOneOutcome *ConcurrentKBisimulationOutcome, minSupport uint64) *ConcurrentKBisimulationOutcome {
	kBlock := kMinOneOutcome.Blocks
	dirtyBlocks := kMinOneOutcome.DirtyBlocks
	kMinOneMapper := kMinOneOutcome.NodeToBlock
	freeBlocks := kMinOneOutcome.freeBlocks
	singletonCount := kMinOneOutcome.singletonCount

	// Reuse the old channels
	freeBlocksChannel := kMinOneOutcome.channels.freeBlocksChannel
	blocksChannel := kMinOneOutcome.channels.blocksChannel
	largeBlocksChannel := kMinOneOutcome.channels.largeBlocksChannel
	singletonsChannel := kMinOneOutcome.channels.singletonsChannel

	// Fill the free blocks channel
	var wgFreeBlocks sync.WaitGroup
	wgFreeBlocks.Add(1)
	go fillFreeBlocksChannel(&freeBlocks, freeBlocksChannel, &wgFreeBlocks)

	// Spawn threads for each block (which in turn will spawn threads for every chunk)
	for _, dirtyBlock := range dirtyBlocks {
		go processBlock(&kBlock, kMinOneMapper, g, dirtyBlock, blocksChannel, freeBlocksChannel, singletonsChannel, largeBlocksChannel)
	}

	// The new Node2BlockMapper
	kMapper := (kMinOneMapper).ModifiableCopy()

	// We want all threads to be done before marking dirty blocks. We achieve this with a waitgroup
	var wgChannelReaders sync.WaitGroup
	wgChannelReaders.Add(3)
	dirtyBlockCount := len(dirtyBlocks)

	// Read the new singletons from the channel and store them locally
	newSingletons := make(Block, 0)
	go readSingletonsChannel(singletonsChannel, &newSingletons, kMapper, dirtyBlockCount, &singletonCount, &wgChannelReaders)

	// Read the new blocks from the channel and store them locally
	newBlocks := make([]BlockPtr, 0)
	go readBlocksChannel(&kBlock, blocksChannel, &newBlocks, freeBlocksChannel, kMapper, dirtyBlockCount, &wgChannelReaders)

	// Read the changed (reused) blocks from the channel and store them locally
	changedBlocks := make([]BlockPtr, 0)
	go readLargeBlocksChannel(largeBlocksChannel, &changedBlocks, dirtyBlockCount, &wgChannelReaders)

	// We use a custom hash set to mark the dirty blocks
	newDirtyBlocksSet := NewBlockHashSet()
	wgChannelReaders.Wait()

	// We first look through all newly created blocks, then we find the nodes pointing to these blocks and finally we mark the blocks containig the respective nodes as dirty
	for _, block := range newBlocks {
		for _, node := range *block {
			for _, parentID := range g.reverse[node] {
				potentialDirtyBlockID := kMapper.GetBlock(parentID)
				// We may get singletons (represented by negative numbers), but we do not need to mark them, since they can not split
				if potentialDirtyBlockID >= 0 {
					newDirtyBlocksSet.Add(uint64(potentialDirtyBlockID)) // This cast is safe as we skip any negative indices
				}
			}
		}
	}
	// Secondly we do the same with the changed blocks (the ones that were overwritten instead of newly created)
	for _, block := range changedBlocks {
		for _, node := range *block {
			for _, parentID := range g.reverse[node] {
				potentialDirtyBlockID := kMapper.GetBlock(parentID)
				// We may get singletons (represented by negative numbers), but we do not need to mark them, since they can not split
				if potentialDirtyBlockID >= 0 {
					newDirtyBlocksSet.Add(uint64(potentialDirtyBlockID)) // This cast is safe as we skip any negative indices
				}
			}
		}
	}

	newKBlock := append(kBlock, newBlocks...)

	newDirtyBlocks := newDirtyBlocksSet.ToSlice()

	// for _, block := range newDirtyBlocks {
	// }

	// Flush freeblocks channel
	// TODO this is in principle not guaranteed to fully empty the channel if threads are still trying to write to it
	wgFreeBlocks.Wait()
	freeBlocksChannelEmpty := false
	for !freeBlocksChannelEmpty {
		select {
		case freeBlock := <-freeBlocksChannel:
			freeBlocks = append(freeBlocks, freeBlock)
		default:
			freeBlocksChannelEmpty = true
		}
	}
	return NewConcurrentKBisimulationOutcome(newKBlock, newDirtyBlocks, kMapper, freeBlocks, kMinOneOutcome.channels, singletonCount)
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
