package store

import "container/list"

type Capacity struct {
	maxKeys  *int
	order    *list.List
	elements map[string]*list.Element
}

func NewCapacity(maxKeys *int) *Capacity {
	return &Capacity{
		maxKeys:  maxKeys,
		order:    list.New(),
		elements: make(map[string]*list.Element),
	}
}

func (capacity *Capacity) Added(key string) {
	if _, exists := capacity.elements[key]; exists {
		return
	}

	capacity.elements[key] = capacity.order.PushBack(key)
}

func (capacity *Capacity) Deleted(key string) {
	element := capacity.elements[key]

	if element == nil {
		return
	}

	capacity.order.Remove(element)
	delete(capacity.elements, key)
}

func (capacity *Capacity) PlanInsert(key string) []string {
	if capacity.maxKeys == nil {
		return nil
	}

	count := len(capacity.elements)

	if _, exists := capacity.elements[key]; !exists {
		count++
	}

	excess := count - *capacity.maxKeys

	if excess <= 0 {
		return nil
	}

	victims := make([]string, 0, excess)

	for element := capacity.order.Front(); element != nil; element = element.Next() {
		if len(victims) == excess {
			break
		}

		if element.Value != key {
			victims = append(victims, element.Value.(string))
		}
	}

	return victims
}

func (capacity *Capacity) Keys() []string {
	keys := make([]string, 0, capacity.order.Len())

	for element := capacity.order.Front(); element != nil; element = element.Next() {
		keys = append(keys, element.Value.(string))
	}

	return keys
}

func (capacity *Capacity) Restore(keys []string) {
	capacity.order.Init()
	clear(capacity.elements)

	for _, key := range keys {
		capacity.Added(key)
	}
}

func (capacity *Capacity) Count() int {
	return len(capacity.elements)
}
