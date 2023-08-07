// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package agg

import (
	"encoding"
	"encoding/binary"
	"encoding/json"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"golang.org/x/exp/constraints"
)

const (
	AggregateSum = iota
	AggregateAvg
	AggregateMax
	AggregateMin
	AggregateCount
	AggregateStarCount
	AggregateApproxCountDistinct
	AggregateVariance
	AggregateBitAnd
	AggregateBitXor
	AggregateBitOr
	AggregateStdDevPop
	AggregateAnyValue
	AggregateMedian
	AggregateGroupConcat
	AggregateGroupConcat1

	WinRank
	WinRowNumber
	WinDenseRank
)

var Names = [...]string{
	AggregateSum:                 "sum",
	AggregateAvg:                 "avg",
	AggregateMax:                 "max",
	AggregateMin:                 "min",
	AggregateCount:               "count",
	AggregateStarCount:           "starcount",
	AggregateApproxCountDistinct: "approx_count_distinct",
	AggregateVariance:            "var",
	AggregateBitAnd:              "bit_and",
	AggregateBitXor:              "bit_xor",
	AggregateBitOr:               "bit_or",
	AggregateStdDevPop:           "stddev_pop",
	AggregateAnyValue:            "any",
	AggregateMedian:              "median",
	AggregateGroupConcat:         "group_concat",

	WinRank:      "rank",
	WinRowNumber: "row_number",
	WinDenseRank: "dense_rank",
}

type Aggregate struct {
	Op     int
	Dist   bool
	E      *plan.Expr
	Config []byte
}

// Agg agg interface
type Agg[T any] interface {
	encoding.BinaryMarshaler
	encoding.BinaryUnmarshaler
	//UnmarshalBinary(data []byte, m *mpool.MPool) error

	// Dup will duplicate a new agg with the same type.
	Dup() Agg[any]

	// Type return the type of the agg's result.
	OutputType() types.Type

	// InputType return the type of the agg's input.
	InputTypes() []types.Type

	// String return related information of the agg.
	// used to show query plans.
	String() string

	// Free the agg.
	Free(*mpool.MPool)

	// Grows allocates n groups for the agg.
	Grows(n int, m *mpool.MPool) error

	// Eval method calculates and returns the final result of the aggregate function.
	Eval(_ *mpool.MPool) (*vector.Vector, error)

	// Fill use the one row of vector to update the data of groupIndex-group's agg.
	Fill(groupIndex int64, rowIndex int64, vecs []*vector.Vector) error

	// BulkFill use whole vector to update the data of groupIndex-group's agg.
	BulkFill(groupIndex int64, rowCount int, vecs []*vector.Vector) error

	// BatchFill use part of the vector to update the data of agg's group
	//      os(origin-s) records information about which groups need to be updated
	//      if length of os is N, we use first N of vps to do update work.
	//      And if os[i] > 0, it means the agg's (vps[i]-1)th group is a new one (never been assigned a value),
	//      Maybe this feature can help us to do some optimization work.
	//      So we use the os as a parameter but not len(os).
	//
	//      agg's (vps[i]-1)th group is related to vector's (offset+i)th row.
	//      rowCounts[i] is count number of the row[i]
	// For a more detailed introduction of rowCounts, please refer to comments of Function Fill.
	BatchFill(offset int64, os []uint8, vps []uint64, vecs []*vector.Vector) error

	// Merge will merge a couple of group between 2 aggregate function structures.
	// It merges the groupIndex1-group of agg1 and
	// groupIndex2-group of agg2
	Merge(agg2 Agg[any], groupIndex1 int64, groupIndex2 int64) error

	// BatchMerge merges multi groups of agg1 and agg2
	//  agg1's (vps[i]-1)th group is related to agg2's (start+i)th group
	// For more introduction of os, please refer to comments of Function BatchFill.
	BatchMerge(agg2 Agg[any], start int64, os []uint8, vps []uint64) error

	// GetInputTypes get types of aggregate's input arguments.
	GetInputTypes() []types.Type

	// GetOperatorId get types of aggregate's aggregate id.
	GetOperatorId() int

	IsDistinct() bool

	// WildAggReAlloc reallocate for agg structure from memory pool.
	WildAggReAlloc(m *mpool.MPool) error
}

type AggStruct interface {
	encoding.BinaryMarshaler
	encoding.BinaryUnmarshaler
}

// UnaryAgg generic aggregation function with one input vector and without distinct
type UnaryAgg[T1, T2 any] struct {
	// operation type of aggregate
	op int

	// aggregate struct
	priv AggStruct

	// vs is result value list
	vs []T2
	// es, es[i] is true to indicate that this group has not yet been populated with any value
	es []bool
	// memory of vs
	da []byte

	// iscount is true,  it means that the aggregation function is count
	isCount bool
	// otyp is output vecotr's type
	otyp types.Type
	// ityps is type list of input vectors
	ityps []types.Type

	// grows used for add groups
	grows func(int)
	// eval used to get final aggregated value
	eval func([]T2, error) ([]T2, error)
	// merge
	// 	first argument is the group number to be merged
	//  second argument is the group number used to merge
	// 	third argument is the value of the group corresponding to the first aggregate function,
	//	fourth argument is the value of the group corresponding to the second aggregate function,
	//  fifth argument is whether the value corresponding to the first aggregate function is empty,
	//  sixth argument is whether the value corresponding to the second aggregate function is empty
	//  seventh value is the private data
	merge func(int64, int64, T2, T2, bool, bool, any) (T2, bool, error)
	// fill
	//  first argument is the group number to be filled
	// 	second parameter is the value to be fed
	//	third is the value of the group to be filled
	// 	fourth is the number of times the first parameter needs to be fed
	//  fifth represents whether it is a new group
	//  sixth represents whether the value to be fed is null
	fill func(int64, T1, T2, int64, bool, bool) (T2, bool, error)

	// Optional optimisation function for functions where cgo is used in a single pass.
	batchFill func(any, any, int64, int64, []uint64, *nulls.Nulls) error

	err error
}

// UnaryDistAgg generic aggregation function with one input vector and with distinct
type UnaryDistAgg[T1, T2 any] struct {
	// operation type of aggregate
	op int

	// aggregate struct
	priv AggStruct

	// vs is result value list
	vs []T2
	// es, es[i] is true to indicate that this group has not yet been populated with any value
	es []bool
	// memory of vs
	da []byte

	// iscount is true,  it means that the aggregation function is count
	isCount bool

	maps []*hashmap.StrHashMap

	// raw values of input vectors
	srcs [][]T1

	// output vecotr's type
	otyp types.Type
	// type list of input vectors
	ityps []types.Type

	// grows used for add groups
	grows func(int)
	// eval used to get final aggregated value
	eval func([]T2, error) ([]T2, error)
	// merge
	// 	first argument is the group number to be merged
	//  second argument is the group number used to merge
	// 	third argument is the value of the group corresponding to the first aggregate function,
	//	fourth argument is the value of the group corresponding to the second aggregate function,
	//  fifth argument is whether the value corresponding to the first aggregate function is empty,
	//  sixth argument is whether the value corresponding to the second aggregate function is empty
	//  seventh value is the private data
	merge func(int64, int64, T2, T2, bool, bool, any) (T2, bool, error)
	// fill
	//  first argument is the group number to be filled
	// 	second parameter is the value to be fed
	//	third is the value of the group to be filled
	// 	fourth is the number of times the first parameter needs to be fed
	//  fifth represents whether it is a new group
	//  sixth represents whether the value to be fed is null
	fill func(int64, T1, T2, int64, bool, bool) (T2, bool, error)

	err error
}

type EncodeAgg struct {
	Op      int
	Private []byte
	Es      []bool
	Da      []byte

	InputTypes []byte
	OutputType []byte
	IsCount    bool
}

type EncodeAggDistinct[T any] struct {
	Op      int
	Private []byte
	Es      []bool
	Da      []byte

	InputTypes []types.Type
	OutputType types.Type

	IsCount bool
	Srcs    [][]T
}

func (m *EncodeAggDistinct[T]) MarshalBinary() ([]byte, error) {
	// ------------------------------
	// | len | m.Srcs | len | aggPB |
	// ------------------------------

	// NOTE: if T was a struct, its private field wouldn't be marshaled
	srcsData, err := json.Marshal(m.Srcs)
	if err != nil {
		return nil, err
	}
	srcsLen := len(srcsData)

	aggPB := EncodeAggDistinctPB{
		Op:         m.Op,
		Private:    m.Private,
		Es:         m.Es,
		Da:         m.Da,
		InputTypes: m.InputTypes,
		OutputType: m.OutputType,
		IsCount:    m.IsCount,
	}
	aggLen := aggPB.ProtoSize()

	// total size for binary format
	size := 4 + srcsLen + 4 + aggLen
	data := make([]byte, size)

	// set length for m.Srcs
	index := 0
	binary.BigEndian.PutUint32(data[index:index+4], uint32(srcsLen))
	index += 4

	// copy data for m.Srcs
	n := copy(data[index:], srcsData)
	if n != srcsLen {
		panic("unexpected length for generics type")
	}
	index += n

	// set length for aggPB
	binary.BigEndian.PutUint32(data[index:index+4], uint32(aggLen))
	index += 4

	// marshal the other fields
	_, err = aggPB.MarshalToSizedBuffer(data[index:])
	if err != nil {
		return nil, err
	}

	return data, nil
}

func (m *EncodeAggDistinct[T]) UnmarshalBinary(data []byte) error {
	// m.Srcs
	l := binary.BigEndian.Uint32(data[:4])
	data = data[4:]

	srcs := [][]T{}
	if err := json.Unmarshal(data[:l], &srcs); err != nil {
		return err
	}
	data = data[l:]
	m.Srcs = srcs

	// other fields
	l = binary.BigEndian.Uint32(data[:4])
	data = data[4:]

	var aggPB EncodeAggDistinctPB
	if err := aggPB.Unmarshal(data[:l]); err != nil {
		return err
	}
	m.Op = aggPB.Op
	m.Private = aggPB.Private
	m.Es = aggPB.Es
	m.Da = aggPB.Da
	m.InputTypes = aggPB.InputTypes
	m.OutputType = aggPB.OutputType
	m.IsCount = aggPB.IsCount

	return nil
}

type Compare interface {
	constraints.Integer | constraints.Float | types.Date |
		types.Datetime | types.Timestamp
}
