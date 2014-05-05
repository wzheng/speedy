// Sorting functions for Whanau

package whanau

import "sort"

// Defines ordering of Record args
type By func(r1, r2 *Record) bool
type ByFinger func(f1, f2 *Finger) bool

// Sort uses By to sort the Record slice
func (by By) Sort(records []Record) {
	rs := &recordSorter{
		records: records,
		by:      by,
	}
	sort.Sort(rs)
}

// sort uses By to sort fingers by id
func (by ByFinger) Sort(fingers []Finger) {
	fs := &fingerSorter{
		fingers: fingers,
		by:      by,
	}
	sort.Sort(fs)
}

// recordSorter joins a By function and a slice of Records to be sorted.
type recordSorter struct {
	records []Record
	by      func(r1, r2 *Record) bool // Closure used in the Less method.
}

// fingerSorter joins a By function and a slice of Fingers to be sorted.
type fingerSorter struct {
	fingers []Finger
	by      func(f1, f2 *Finger) bool // Closure used in the Less method
}

// Len is part of sort.Interface.
func (s *recordSorter) Len() int {
	return len(s.records)
}

func (s *fingerSorter) Len() int {
	return len(s.fingers)
}

// Swap is part of sort.Interface.
func (s *recordSorter) Swap(i, j int) {
	s.records[i], s.records[j] = s.records[j], s.records[i]
}

func (s *fingerSorter) Swap(i, j int) {
	s.fingers[i], s.fingers[j] = s.fingers[j], s.fingers[i]
}

// Less is part of sort.Interface. It is implemented by calling the "by" closure in the sorter.
func (s *recordSorter) Less(i, j int) bool {
	return s.by(&s.records[i], &s.records[j])
}

func (s *fingerSorter) Less(i, j int) bool {
	return s.by(&s.fingers[i], &s.fingers[j])
}

var RecordKey = func(r1, r2 *Record) bool {
	return r1.Key < r2.Key
}

var FingerId = func(f1, f2 *Finger) bool {
	return f1.Id < f2.Id
}
