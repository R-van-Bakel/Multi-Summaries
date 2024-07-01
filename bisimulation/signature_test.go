package bisimulation

import (
	"bufio"
	"fmt"
	"math/rand"
	"os"
	"testing"
)

func TestSignatureEqualsAndHash(t *testing.T) {
	for _, deduplicate := range []bool{true, false} {
		s1b := NewSignatureBuilder(deduplicate)
		s2b := NewSignatureBuilder(deduplicate)

		for labelID := 0; labelID < 100; labelID++ {
			for blockID := 123; blockID < 140; blockID++ {
				s1b.AddPiece(uint32(labelID), int64(blockID))
				s2b.AddPiece(uint32(labelID), int64(blockID))
				s1 := s1b.Build()
				s2 := s2b.Build()
				if !s1.Equals(s2) {
					t.Fatal("Signatures that must be equal did not compare equal")
				}
				if s1.Hash() != s2.Hash() {
					t.Fatal("Signatures that must be equal did not give the same hash")
				}
			}
		}
		// add all again to test whether deduplication works as expected
		for labelID := 0; labelID < 100; labelID++ {
			for _, blockID := range []int64{123, 128, 128, 130} {
				s1b.AddPiece(uint32(labelID), blockID)
				s2b.AddPiece(uint32(labelID), blockID)
				s1 := s1b.Build()
				s2 := s2b.Build()
				if !s1.Equals(s2) {
					t.Fatal("Signatures that must be equal did not compare equal")
				}
				if s1.Hash() != s2.Hash() {
					t.Fatal("Signatures that must be equal did not give the same hash")
				}
			}
		}
	}

}

func TestUniqSortedDestructive(t *testing.T) {
	rng := rand.New(rand.NewSource(4564))

	for length := 0; length < 100; length++ {
		for _retry := 0; _retry < 5; _retry++ {

			items := make([]int, 0, length)
			previous := 0
			first := true
			for i := 0; i < length; i++ {
				// duplicate or not?
				if first {
					previous = 1
				}

				duplicate := rng.Intn(2) == 1
				if !duplicate && !first {
					previous += 1
				}
				items = append(items, previous)
				first = false
			}
			items = uniqSortedDestructive(items)
			if len(items) != previous {
				t.Fatal("The number of deduplicated items is not as expected. ")
			}
			for i := 0; i < previous-1; i++ { // -1 because we check with the next one
				if items[i] == items[i+1] {
					t.Fatal("Did not correctly deduplicate")
				}
			}
		}
	}
}

func BenchmarkUniqSortedDestructive(outer_b *testing.B) {
	rng := rand.New(rand.NewSource(4564))

	for length := 0; length < 100000; length += 10000 {
		for retry := 0; retry < 5; retry++ {
			outer_b.Run(fmt.Sprintf("input_size_%d_retry_%d", length, retry),

				func(b *testing.B) {
					b.StopTimer()
					items := make([]int, 0, length)
					previous := 0
					first := true
					for i := 0; i < length; i++ {
						// duplicate or not?
						if first {
							previous = 1
						}

						duplicate := rng.Intn(2) == 1
						if !duplicate && !first {
							previous += 1
						}
						items = append(items, previous)
						first = false
					}
					b.StartTimer()
					_ = uniqSortedDestructive(items)

				})
		}
	}
}

func dedup[V interface {
	comparable
}](original []V) []V {
	newList := make([]V, 0, len(original))
	entries := make(map[V]struct{})
	for _, value := range original {
		_, ok := entries[value]
		if !ok {
			entries[value] = struct{}{}
			newList = append(newList, value)
		}
	}
	return newList
}

func TestSignatureBlockMapCreation(t *testing.T) {
	var amount int
	if testing.Short() {
		amount = 8
	} else {
		amount = 9
	}
	labels := []uint32{0, 1, 1, 2, 3, 4, 4, 5}
	for i := 1; i < amount; i++ {
		more := make([]uint32, 0, len(labels))
		for _, label := range labels {
			more = append(more, label*uint32(i))
		}
		labels = append(labels, more...)
	}
	blocks := []uint64{5000, 5100, 5000, 5100, 5100, 5500, 5600, 12500}
	for i := 1; i < amount; i++ {
		more := make([]uint64, 0, len(blocks))
		for _, block := range blocks {
			more = append(more, block*uint64(i))
		}
		blocks = append(blocks, more...)
	}
	for _, deduplicate := range []bool{true, false} {

		for lastLabel := 1; lastLabel <= len(labels); lastLabel *= 2 {
			for lastBlock := 1; lastBlock <= len(blocks); lastBlock *= 2 {
				// We make simple signatures and add them to the SBM
				sbm := NewSignatureBlockMap()
				for _, label := range labels[:lastLabel] {
					for _, node := range blocks[:lastBlock] {
						sb := NewSignatureBuilder(deduplicate)
						sb.AddPiece(label, int64(node))
						sbm.Put(sb.Build(), uint64(label)*node)
					}
				}
				// Check whether the number of blocks is as expected
				// since we use only one combination for each signature,
				// we expect the number of blocks to be the number of unique
				// labels * number of unique blocks

				uniqueLabelCount := len(dedup(labels[:lastLabel]))
				uniqueNodeCount := len(dedup(blocks[:lastBlock]))

				newBlocksCount := len(sbm.GetBlocks())
				if newBlocksCount != uniqueLabelCount*uniqueNodeCount {
					t.Fatal("The number of new parts determined by sbm was not as expected.")
				}
			}
		}

	}

}

func TestSignatureBlockMapMerge(t *testing.T) {
	labels := []uint32{0, 1, 1, 2, 3, 4, 4, 5}
	for i := 1; i < 8; i++ {
		more := make([]uint32, 0, len(labels))
		for _, label := range labels {
			more = append(more, label*uint32(i))
		}
		labels = append(labels, more...)
	}
	blocks := []uint64{5000, 5100, 5000, 5100, 5100, 5500, 5600, 12500}
	for i := 1; i < 8; i++ {
		more := make([]uint32, 0, len(labels))
		for _, label := range labels {
			more = append(more, label*uint32(i))
		}
		labels = append(labels, more...)
	}
	for _, deduplicate := range []bool{true, false} {
		for lastLabel := 1; lastLabel <= len(labels); lastLabel *= 2 {
			for lastBlock := 1; lastBlock <= len(blocks); lastBlock *= 2 {
				// We make simple signatures and add them to the SBM
				sbm := NewSignatureBlockMap()

				// we make 2 more sbms and add the signatures to that, and merge them in
				sbm1 := NewSignatureBlockMap()

				for _, label := range labels[:lastLabel] {
					for _, node := range blocks[:(lastBlock / 2)] {
						sb := NewSignatureBuilder(deduplicate)
						sb.AddPiece(label, int64(node))
						sbm1.Put(sb.Build(), uint64(label)*node)
					}
				}

				sbm2 := NewSignatureBlockMap()

				for _, label := range labels[:lastLabel] {
					for _, node := range blocks[(lastBlock / 2):lastBlock] {
						sb := NewSignatureBuilder(deduplicate)
						sb.AddPiece(label, int64(node))
						sbm2.Put(sb.Build(), uint64(label)*node)
					}
				}

				sbm.MergeDestructive(&sbm1)
				sbm.MergeDestructive(&sbm2)

				// Check whether the number of blocks is as expected
				// since we use only one combination for each signature,
				// we expect the number of blocks to be the number of unique
				// labels * number of unique blocks

				uniqueLabelCount := len(dedup(labels[:lastLabel]))
				uniqueNodeCount := len(dedup(blocks[:lastBlock]))

				newBlocksCount := len(sbm.GetBlocks())
				if newBlocksCount != uniqueLabelCount*uniqueNodeCount {
					t.Fatal("The number of new parts determined by sbm was not as expected.")
				}
			}
		}

	}

}

func TestPut(t *testing.T) {
	m := NewSignatureBlockMap()

	sortedPieces1 := make([]SignaturePiece, 0)
	sortedPieces1 = append(sortedPieces1, SignaturePiece{label: 0, block: 0}, SignaturePiece{label: 0, block: 1}, SignaturePiece{label: 1, block: 2})
	var signature1 *Signature = NewSignature(sortedPieces1)

	sortedPieces2 := make([]SignaturePiece, 0)
	sortedPieces2 = append(sortedPieces2, SignaturePiece{label: 3, block: 2}, SignaturePiece{label: 2, block: 1})
	var signature2 *Signature = NewSignature(sortedPieces2)

	node1 := nodeIndex(3)
	node2 := nodeIndex(7)
	node3 := nodeIndex(0)
	node4 := nodeIndex(15)

	m.Put(signature1, node1)
	m.Put(signature1, node2)
	m.Put(signature2, node3)
	m.Put(signature2, node4)

	// We expect the following blocks: [[3,7][0,15]]
	if len(m.GetBlocks()) == 2 && len(m.GetBlocks()[0]) == 2 && len(m.GetBlocks()[1]) == 2 {
		if m.GetBlocks()[0][0] == 3 && m.GetBlocks()[0][1] == 7 && m.GetBlocks()[1][0] == 0 && m.GetBlocks()[1][1] == 15 {
			return
		}
	}
	t.Fatal("The result was not as expected")
}

// ###### Integration tests ######

func TestConcurrentStepZero(t *testing.T) {
	// Open the binary graph file
	graphFile, err := os.Open("./test_data/Fishburne_binary_encoding.bin")
	if err != nil {
		logger.Println("Error while opening the graph binary")
		t.Fatal(err)
	}

	// Try to read off the meta data from the binary
	graphFileInfo, err := graphFile.Stat()
	if err != nil {
		logger.Println("Error while getting the size of the graph binary")
		t.Fatal(err)
	}

	// A check for if the data is stored correctly
	if graphFileInfo.Size()%14 != 0 {
		t.Fatal("The graph binary was not a multiple of 14 bytes (5 for subject, 4 for relation, 5 for object)")
	}

	// Get and print the graph size
	graphSize := graphFileInfo.Size() / 14
	logger.Printf("Graph size: %d triples\n", graphSize)

	// We make a rough estimate of how many nodes are in the graph
	nodeCountEstimate := graphSize // This would assume a sparse graph where on avergage there is one new node per triple

	// Create a new graph object with some memory pre-allocated
	g := NewGraph(nodeCountEstimate)

	// Read the binary data into the graph
	bufReader := bufio.NewReaderSize(graphFile, 1<<14)
	g.ReadFromStream(bufReader)

	// Calculate the reverse index
	g.CreateReverseIndex()

	// Perform the zeroth step in our k-forward bisimulation algorithm
	MultiThreadKBisimulationStepZero(g)
}

func TestConcurrentStepOne(t *testing.T) {
	// Open the binary graph file
	graphFile, err := os.Open("./test_data/Fishburne_binary_encoding.bin")
	if err != nil {
		logger.Println("Error while opening the graph binary")
		t.Fatal(err)
	}

	// Try to read off the meta data from the binary
	graphFileInfo, err := graphFile.Stat()
	if err != nil {
		logger.Println("Error while getting the size of the graph binary")
		t.Fatal(err)
	}

	// A check for if the data is stored correctly
	if graphFileInfo.Size()%14 != 0 {
		t.Fatal("The graph binary was not a multiple of 14 bytes (5 for subject, 4 for relation, 5 for object)")
	}

	// Get and print the graph size
	graphSize := graphFileInfo.Size() / 14
	logger.Printf("Graph size: %d triples\n", graphSize)

	// We make a rough estimate of how many nodes are in the graph
	nodeCountEstimate := graphSize // This would assume a sparse graph where on avergage there is one new node per triple

	// Create a new graph object with some memory pre-allocated
	g := NewGraph(nodeCountEstimate)

	// Read the binary data into the graph
	bufReader := bufio.NewReaderSize(graphFile, 1<<14)
	g.ReadFromStream(bufReader)

	// Calculate the reverse index
	g.CreateReverseIndex()

	// Perform the zeroth step in our k-forward bisimulation algorithm
	outcomeK := MultiThreadKBisimulationStepZero(g)

	// Set the minimum support parameter
	var minSupport uint64 = 0

	// Perform a proper bisimulation step
	MultiThreadKBisimulationStep(g, outcomeK, minSupport)
}

type ConcurrentTestResults struct {
	blockCounts     []nodeIndex
	singletonCounts []nodeIndex
	fixedPoint      uint32
}

func NewConcurrentTestResults(blockCounts []nodeIndex, singletonCounts []nodeIndex, fixedPoint uint32) ConcurrentTestResults {
	return ConcurrentTestResults{
		blockCounts:     blockCounts,
		singletonCounts: singletonCounts,
		fixedPoint:      fixedPoint,
	}
}

func compareResults(results ConcurrentTestResults, expectedResults ConcurrentTestResults, t *testing.T) {
	if results.fixedPoint != expectedResults.fixedPoint {
		t.Fatal("Mismatched fixed point")
	}
	var i uint32
	for i = 0; i > results.fixedPoint+1; i++ {
		if results.blockCounts[i] != expectedResults.blockCounts[i] {
			t.Fatal("Mismatched block counts")
		}
		if results.singletonCounts[i] != expectedResults.singletonCounts[i] {
			t.Fatal("Mismatched singleton counts")
		}
	}
}

func RunConcurrent(file_path string, t *testing.T) ConcurrentTestResults {
	// Open the binary graph file
	graphFile, err := os.Open(file_path)
	if err != nil {
		logger.Println("Error while opening the graph binary")
		t.Fatal(err)
	}

	// Try to read off the meta data from the binary
	graphFileInfo, err := graphFile.Stat()
	if err != nil {
		logger.Println("Error while getting the size of the graph binary")
		t.Fatal(err)
	}

	// A check for if the data is stored correctly
	if graphFileInfo.Size()%14 != 0 {
		t.Fatal("The graph binary was not a multiple of 14 bytes (5 for subject, 4 for relation, 5 for object)")
	}

	// Get and print the graph size
	graphSize := graphFileInfo.Size() / 14
	logger.Printf("Graph size: %d triples\n", graphSize)

	// We make a rough estimate of how many nodes are in the graph
	nodeCountEstimate := graphSize // This would assume a sparse graph where on avergage there is one new node per triple

	// Create a new graph object with some memory pre-allocated
	g := NewGraph(nodeCountEstimate)

	// Read the binary data into the graph
	bufReader := bufio.NewReaderSize(graphFile, 1<<14)
	g.ReadFromStream(bufReader)

	// Calculate the reverse index
	g.CreateReverseIndex()

	logger.Printf("Graph size: %d nodes\n", g.GetSize())

	// Perform the zeroth step in our k-forward bisimulation algorithm
	outcomeK := MultiThreadKBisimulationStepZero(g)

	// Set the minimum support parameter
	var minSupport uint64 = 0

	blockCounts := make([]nodeIndex, 0)
	singletonCounts := make([]nodeIndex, 0)

	// Perform the full bisimulation
	var k uint32 = 0
	var blockCount nodeIndex
	var singletonCount nodeIndex
	var previousBlockCount nodeIndex = 0
	var previousSingletonCount nodeIndex = 0
	for {
		outcomeK = MultiThreadKBisimulationStep(g, outcomeK, minSupport)
		k++
		blockCount = nodeIndex(len((*outcomeK).Blocks)) - nodeIndex(len((*outcomeK).freeBlocks))
		singletonCount = nodeIndex((*outcomeK).singletonCount)
		logger.Printf("(k=%d) Block count:     %d\n", k, blockCount)
		logger.Printf("(k=%d) Singelton count: %d\n\n", k, singletonCount)
		blockCounts = append(blockCounts, blockCount)
		singletonCounts = append(singletonCounts, singletonCount)
		if blockCount == previousBlockCount && singletonCount == previousSingletonCount {
			break
		}
		previousBlockCount = blockCount
		previousSingletonCount = singletonCount
	}
	logger.Printf("Ran bisimulation on %s for k=%d levels (i.e. k=%d should be a fixed point).\n\n", file_path, k, k-1)
	result := NewConcurrentTestResults(blockCounts, singletonCounts, k-1)
	return result
}

func TestConcurrent(t *testing.T) {
	simpleChainExpectedResult := NewConcurrentTestResults(
		[]nodeIndex{1, 1, 1, 1, 1, 1, 1, 0, 0},
		[]nodeIndex{1, 2, 3, 4, 5, 6, 7, 9, 9},
		8)
	simpleChainResult := RunConcurrent("./test_data/simple_chain_binary_encoding.bin", t)
	compareResults(simpleChainResult, simpleChainExpectedResult, t)

	simpleCycleExpectedResult := NewConcurrentTestResults(
		[]nodeIndex{1, 1},
		[]nodeIndex{1, 1},
		1)
	simpleCycleResult := RunConcurrent("./test_data/simple_cycle_binary_encoding.bin", t)
	compareResults(simpleCycleResult, simpleCycleExpectedResult, t)

	simpleCyclesExpectedResult := NewConcurrentTestResults(
		[]nodeIndex{1, 1, 1, 2, 1, 1},
		[]nodeIndex{1, 2, 3, 3, 5, 5},
		5)
	simpleCyclesResult := RunConcurrent("./test_data/simple_cycles_binary_encoding.bin", t)
	compareResults(simpleCyclesResult, simpleCyclesExpectedResult, t)

	simpleDagExpectedResult := NewConcurrentTestResults(
		[]nodeIndex{1, 1, 1, 2, 3, 3},
		[]nodeIndex{1, 2, 3, 3, 3, 3},
		5)
	simpleDagResult := RunConcurrent("./test_data/simple_dag_binary_encoding.bin", t)
	compareResults(simpleDagResult, simpleDagExpectedResult, t)

	simpleEdgeExpectedResult := NewConcurrentTestResults(
		[]nodeIndex{0, 0},
		[]nodeIndex{2, 2},
		1)
	simpleEdgeResult := RunConcurrent("./test_data/simple_edge_binary_encoding.bin", t)
	compareResults(simpleEdgeResult, simpleEdgeExpectedResult, t)

	toxicExpectedResult := NewConcurrentTestResults(
		[]nodeIndex{2, 3, 4, 5, 5},
		[]nodeIndex{0, 0, 0, 0, 0},
		4)
	toxicResult := RunConcurrent("./test_data/toxic_binary_encoding.bin", t)
	compareResults(toxicResult, toxicExpectedResult, t)
}
