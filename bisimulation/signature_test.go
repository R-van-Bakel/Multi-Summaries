package bisimulation

import (
	"testing"
)

func TestSignatureEqualsAndHash(t *testing.T) {
	for _, deduplicate := range []bool{true, false} {
		s1b := NewSignatureBuilder(deduplicate)
		s2b := NewSignatureBuilder(deduplicate)

		for labelID := 0; labelID < 100; labelID++ {
			for blockID := 123; blockID < 140; blockID++ {
				s1b.AddPiece(uint32(labelID), uint64(blockID))
				s2b.AddPiece(uint32(labelID), uint64(blockID))
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
			for _, blockID := range []uint64{123, 128, 128, 130} {
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
						sb.AddPiece(label, node)
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
						sb.AddPiece(label, node)
						sbm1.Put(sb.Build(), uint64(label)*node)
					}
				}

				sbm2 := NewSignatureBlockMap()

				for _, label := range labels[:lastLabel] {
					for _, node := range blocks[(lastBlock / 2):lastBlock] {
						sb := NewSignatureBuilder(deduplicate)
						sb.AddPiece(label, node)
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
