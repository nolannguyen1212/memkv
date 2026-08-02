package store

type Deque[T any] struct {
	elements []T
	head     int
	tail     int
	length   int
	capacity int
}

func NewDeque[T any]() *Deque[T] {
	return &Deque[T]{
		elements: make([]T, 4),
		capacity: 4,
	}
}

func (d *Deque[T]) IsEmpty() bool { return d.length == 0 }
func (d *Deque[T]) Length() int   { return d.length }

func (d *Deque[T]) PushFront(elem T) {
	if d.length == d.capacity {
		d.grow()
	}
	d.head = (d.head - 1 + d.capacity) % d.capacity
	d.elements[d.head] = elem
	d.length++
}

func (d *Deque[T]) PushBack(elem T) {
	if d.length == d.capacity {
		d.grow()
	}
	d.elements[d.tail] = elem
	d.tail = (d.tail + 1) % d.capacity
	d.length++
}

func (d *Deque[T]) PopFront() (T, bool) {
	var zero T
	if d.IsEmpty() {
		return zero, false
	}
	elem := d.elements[d.head]
	d.elements[d.head] = zero
	d.head = (d.head + 1) % d.capacity
	d.length--
	return elem, true
}

func (d *Deque[T]) PopBack() (T, bool) {
	var zero T
	if d.IsEmpty() {
		return zero, false
	}
	d.tail = (d.tail - 1 + d.capacity) % d.capacity
	elem := d.elements[d.tail]
	d.elements[d.tail] = zero
	d.length--
	return elem, true
}

func (d *Deque[T]) At(index int) T {
	return d.elements[(d.head+index)%d.capacity]
}

func (d *Deque[T]) grow() {
	newCap := d.capacity * 2
	newElems := make([]T, newCap)
	for i := 0; i < d.length; i++ {
		newElems[i] = d.At(i)
	}
	d.elements = newElems
	d.capacity = newCap
	d.head = 0
	d.tail = d.length
}
