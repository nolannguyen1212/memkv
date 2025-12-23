package storage

import "sort"

const defaultDequeCapacity = 4

type Deque[T any] struct {
	elements []T
	head     int
	tail     int
	length   int
	capacity int
}

func NewDeque[T any]() *Deque[T] {
	return &Deque[T]{
		elements: make([]T, defaultDequeCapacity),
		capacity: defaultDequeCapacity,
		head:     0,
		tail:     0,
		length:   0,
	}
}

func (deque *Deque[T]) IsEmpty() bool {
	return deque.length == 0
}

func (deque *Deque[T]) Length() int {
	return deque.length
}

func (deque *Deque[T]) PushFront(element T) {
	if deque.length == deque.capacity {
		deque.grow()
	}

	deque.head = deque.wrapIndex(deque.head - 1)
	deque.elements[deque.head] = element
	deque.length++
}

func (deque *Deque[T]) PushBack(element T) {
	if deque.length == deque.capacity {
		deque.grow()
	}

	deque.elements[deque.tail] = element
	deque.tail = deque.wrapIndex(deque.tail + 1)
	deque.length++
}

func (deque *Deque[T]) PopFront() (T, bool) {
	var zeroValue T
	if deque.IsEmpty() {
		return zeroValue, false
	}

	element := deque.elements[deque.head]
	deque.elements[deque.head] = zeroValue
	deque.head = deque.wrapIndex(deque.head + 1)
	deque.length--

	return element, true
}

func (deque *Deque[T]) PopBack() (T, bool) {
	var zeroValue T
	if deque.IsEmpty() {
		return zeroValue, false
	}

	deque.tail = deque.wrapIndex(deque.tail - 1)
	element := deque.elements[deque.tail]
	deque.elements[deque.tail] = zeroValue
	deque.length--

	return element, true
}

func (deque *Deque[T]) ElementAt(index int) T {
	position := deque.wrapIndex(deque.head + index)
	return deque.elements[position]
}

func (deque *Deque[T]) Sort(less func(first, second T) bool) {
	if deque.IsEmpty() {
		return
	}

	sortedElements := make([]T, deque.length)
	for i := 0; i < deque.length; i++ {
		sortedElements[i] = deque.ElementAt(i)
	}

	sort.Slice(sortedElements, func(i, j int) bool {
		return less(sortedElements[i], sortedElements[j])
	})

	deque.head = 0
	deque.tail = deque.length
	for i := 0; i < deque.length; i++ {
		deque.elements[i] = sortedElements[i]
	}
}

func (deque *Deque[T]) wrapIndex(index int) int {
	return (index + deque.capacity) % deque.capacity
}

func (deque *Deque[T]) grow() {
	newCapacity := deque.capacity * 2
	newElements := make([]T, newCapacity)

	for i := 0; i < deque.length; i++ {
		newElements[i] = deque.ElementAt(i)
	}

	deque.elements = newElements
	deque.capacity = newCapacity
	deque.head = 0
	deque.tail = deque.length
}
