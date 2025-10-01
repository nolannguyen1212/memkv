package datastructure

import "sort"

type Deque[T any] struct {
	items    []T
	head     int
	tail     int
	size     int
	capacity int
}

func NewDeque[T any]() *Deque[T] {
	capacity := 4
	return &Deque[T]{
		items:    make([]T, capacity),
		capacity: capacity,
		head:     0,
		tail:     0,
		size:     0,
	}
}

func (d *Deque[T]) Empty() bool {
	return d.size == 0
}

func (d *Deque[T]) Size() int {
	return d.size
}

func (d *Deque[T]) Shift(pos int, steps int) int {
	return (pos + steps + d.capacity) % d.capacity
}

func (d *Deque[T]) ShiftLeft(pos int) int {
	return (pos - 1 + d.capacity) % d.capacity
}

func (d *Deque[T]) ShiftRight(pos int) int {
	return (pos + 1) % d.capacity
}

func (d *Deque[T]) resize() {
	cap := 2 * d.capacity
	items := make([]T, cap)

	for i := 0; i < d.size; i++ {
		items[i] = d.items[(d.head+i)%d.capacity]
	}

	d.items = items
	d.capacity = cap
	d.head = 0
	d.tail = d.size
}

func (d *Deque[T]) PushFront(item T) {
	if d.size == d.capacity {
		d.resize()
	}
	d.head = d.ShiftLeft(d.head)
	d.items[d.head] = item
	d.size++
}

func (d *Deque[T]) PushBack(item T) {
	if d.size == d.capacity {
		d.resize()
	}
	d.items[d.tail] = item
	d.tail = d.ShiftRight(d.tail)
	d.size++
}

func (d *Deque[T]) PopFront() (T, bool) {
	var zero T
	if d.Empty() {
		return zero, false
	}
	value := d.items[d.head]
	d.items[d.head] = zero
	d.head = d.ShiftRight(d.head)
	d.size--
	return value, true
}

func (d *Deque[T]) PopBack() (T, bool) {
	var zero T
	if d.Empty() {
		return zero, false
	}

	d.tail = d.ShiftLeft(d.tail)
	value := d.items[d.tail]
	d.items[d.tail] = zero
	d.size--
	return value, true
}

func (d *Deque[T]) Sort(less func(i, j T) bool) {
	if d.Empty() {
		return
	}

	temp := make([]T, d.size)
	for i := 0; i < d.size; i++ {
		pos := (d.head + i) % d.capacity
		temp[i] = d.items[pos]
	}

	sort.Slice(temp, func(i, j int) bool {
		return less(temp[i], temp[j])
	})

	d.head = 0
	d.tail = d.size
	for i := 0; i < d.size; i++ {
		d.items[i] = temp[i]
	}
}
