package bisimulation

import (
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

func TestConcurrentStepZero(t *testing.T) {
	graphFile, err := os.Open("../output/Laurence_Fishburne_Custom_Shuffled_Extra_Edges/binary_encoding.bin")
	if err != nil {
		fmt.Println("Error while opening the graph binary")
		panic(err)
	}

	graphFileInfo, err := graphFile.Stat()
	if err != nil {
		fmt.Println("Error while getting the size of the graph binary")
		panic(err)
	}

	if graphFileInfo.Size()%14 != 0 {
		panic("The graph binary was not a multiple of 14 bytes")
	}

	graphSize := graphFileInfo.Size() / 14
	g := NewGraph(graphSize)

	g.ReadFromStream(graphFile)

	MultiThreadKBisimulationStepZero(g)
}
