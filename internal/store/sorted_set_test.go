package store

import (
	"fmt"
	"math/rand"
	"slices"
	"sort"
	"testing"
)

func TestSortedSetMatchesReferenceModel(t *testing.T) {
	random := rand.New(rand.NewSource(42))
	database := New(Config{})
	model := make(map[string]float64)

	for step := 0; step < 5_000; step++ {
		member := fmt.Sprintf("member-%d", random.Intn(20))
		if random.Intn(4) == 0 {
			database.SortedSet.Remove("scores", member)
			delete(model, member)
			if len(model) == 0 {
				database.RemoveEmptyKey("scores", KeyTypeSortedSet)
			}
		} else {
			score := float64(random.Intn(11) - 5)
			database.SortedSet.Add("scores", ScoreMember{Score: score, Member: member})
			model[member] = score
		}

		expected := sortedMembers(model)
		actual := database.SortedSet.Range("scores", 0, -1)
		if !slices.Equal(actual, expected) {
			t.Fatalf("step %d: range = %v, want %v", step, actual, expected)
		}
		for rank, expectedMember := range expected {
			actualRank, exists := database.SortedSet.Rank("scores", expectedMember)
			if !exists || actualRank != rank {
				t.Fatalf("step %d: rank(%s) = %d, %v; want %d, true", step, expectedMember, actualRank, exists, rank)
			}
			actualScore, exists := database.SortedSet.Score("scores", expectedMember)
			if !exists || actualScore != model[expectedMember] {
				t.Fatalf("step %d: score(%s) = %v, %v; want %v, true", step, expectedMember, actualScore, exists, model[expectedMember])
			}
		}
	}
}

func TestSortedSetRangeBoundaries(t *testing.T) {
	database := New(Config{})
	database.SortedSet.Add("scores",
		ScoreMember{Score: 1, Member: "b"},
		ScoreMember{Score: 1, Member: "a"},
		ScoreMember{Score: 2, Member: "c"},
	)
	tests := []struct {
		start int
		stop  int
		want  []string
	}{
		{0, -1, []string{"a", "b", "c"}},
		{-2, -1, []string{"b", "c"}},
		{-10, 1, []string{"a", "b"}},
		{3, 4, []string{}},
		{2, 1, []string{}},
	}
	for _, test := range tests {
		if actual := database.SortedSet.Range("scores", test.start, test.stop); !slices.Equal(actual, test.want) {
			t.Fatalf("Range(%d, %d) = %v, want %v", test.start, test.stop, actual, test.want)
		}
	}
}

func sortedMembers(scores map[string]float64) []string {
	items := make([]ScoreMember, 0, len(scores))
	for member, score := range scores {
		items = append(items, ScoreMember{Score: score, Member: member})
	}
	sort.Slice(items, func(left, right int) bool {
		if items[left].Score == items[right].Score {
			return items[left].Member < items[right].Member
		}
		return items[left].Score < items[right].Score
	})
	members := make([]string, len(items))
	for index, item := range items {
		members[index] = item.Member
	}
	return members
}

func BenchmarkSortedSetAdd(b *testing.B) {
	for _, cardinality := range []int{100, 1_000, 10_000} {
		b.Run(fmt.Sprintf("cardinality-%d", cardinality), func(b *testing.B) {
			database := New(Config{})
			items := make([]ScoreMember, cardinality)
			for index := 0; index < cardinality; index++ {
				items[index] = ScoreMember{Score: float64(index), Member: fmt.Sprintf("member-%d", index)}
			}
			database.SortedSet.Add("scores", items...)
			b.ResetTimer()
			highScore := true
			for b.Loop() {
				score := -1.0
				if highScore {
					score = float64(cardinality)
				}
				database.SortedSet.Add("scores", ScoreMember{Score: score, Member: "member-0"})
				highScore = !highScore
			}
		})
	}
}
