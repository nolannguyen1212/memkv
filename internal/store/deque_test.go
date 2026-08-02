package store

import (
	"math/rand"
	"slices"
	"testing"
)

func TestDequeMatchesSliceModel(t *testing.T) {
	random := rand.New(rand.NewSource(42))
	deque := NewDeque[int]()
	model := []int{}

	for step := range 10_000 {
		value := random.Int()

		switch random.Intn(4) {
		case 0:
			deque.PushFront(value)
			model = append([]int{value}, model...)
		case 1:
			deque.PushBack(value)
			model = append(model, value)
		case 2:
			actual, exists := deque.PopFront()

			if len(model) == 0 {
				if exists {
					t.Fatalf("step %d: PopFront succeeded on empty deque", step)
				}

				break
			}

			if !exists || actual != model[0] {
				t.Fatalf("step %d: PopFront = %d, %v; want %d, true", step, actual, exists, model[0])
			}

			model = model[1:]
		case 3:
			actual, exists := deque.PopBack()

			if len(model) == 0 {
				if exists {
					t.Fatalf("step %d: PopBack succeeded on empty deque", step)
				}

				break
			}

			last := len(model) - 1

			if !exists || actual != model[last] {
				t.Fatalf("step %d: PopBack = %d, %v; want %d, true", step, actual, exists, model[last])
			}

			model = model[:last]
		}

		actual := make([]int, deque.Length())

		for index := range actual {
			actual[index] = deque.at(index)
		}

		if !slices.Equal(actual, model) {
			t.Fatalf("step %d: deque = %v, model = %v", step, actual, model)
		}
	}
}

func TestDequeRejectsInvalidIndex(t *testing.T) {
	deque := NewDeque[int]()
	deque.PushBack(1)

	for _, index := range []int{-1, 1} {
		if _, exists := deque.At(index); exists {
			t.Fatalf("At(%d) succeeded", index)
		}
	}
}
