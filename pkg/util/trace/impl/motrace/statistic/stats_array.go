// Copyright 2021 - 2023 Matrix Origin
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

package statistic

import (
	"context"
	"strconv"
	"time"
)

type StatsArray [StatsArrayLength]float64

const (
	Decimal128ToFloat64Scale = 5
	Float64PrecForMemorySize = 3
)

const (
	StatsArrayVersion = StatsArrayVersion3

	StatsArrayVersion0 = 0 // raw statistics
	StatsArrayVersion1 = 1 // float64 array
	StatsArrayVersion2 = 2 // float64 array + plus one elem OutTrafficBytes
	StatsArrayVersion3 = 3 // ... + one elem: ConnType
)

const (
	StatsArrayIndexVersion = iota
	StatsArrayIndexTimeConsumed
	StatsArrayIndexMemorySize
	StatsArrayIndexS3IOInputCount
	StatsArrayIndexS3IOOutputCount // index: 4
	StatsArrayIndexOutTrafficBytes // index: 5
	StatsArrayIndexConnType        // index: 6

	StatsArrayLength
)

const (
	StatsArrayLengthV1 = 5
	StatsArrayLengthV2 = 6
	StatsArrayLengthV3 = 7
)

type ConnType float64

const (
	ConnTypeUnknown  ConnType = 0
	ConnTypeInternal ConnType = 1
	ConnTypeExternal ConnType = 2
)

func NewStatsArray() *StatsArray {
	var s StatsArray
	return s.Init()
}

func NewStatsArrayV1() *StatsArray {
	return NewStatsArray().WithVersion(StatsArrayVersion1)
}

func NewStatsArrayV2() *StatsArray {
	return NewStatsArray().WithVersion(StatsArrayVersion2)
}

func NewStatsArrayV3() *StatsArray {
	return NewStatsArray()
}

func (s *StatsArray) Init() *StatsArray {
	return s.WithVersion(StatsArrayVersion)
}

func (s *StatsArray) InitIfEmpty() *StatsArray {
	for i := 1; i < StatsArrayLength; i++ {
		if s[i] != 0 {
			return s
		}
	}
	return s.WithVersion(StatsArrayVersion)
}

func (s *StatsArray) Reset() *StatsArray {
	return s.WithVersion(StatsArrayVersion).
		// StatsArrayVersion1
		WithTimeConsumed(0).WithMemorySize(0).WithS3IOInputCount(0).WithS3IOOutputCount(0).
		// StatsArrayVersion2
		WithOutTrafficBytes(0)
	// Next Version
}

func (s *StatsArray) GetVersion() float64         { return (*s)[StatsArrayIndexVersion] }
func (s *StatsArray) GetTimeConsumed() float64    { return (*s)[StatsArrayIndexTimeConsumed] }    // unit: ns
func (s *StatsArray) GetMemorySize() float64      { return (*s)[StatsArrayIndexMemorySize] }      // unit: byte
func (s *StatsArray) GetS3IOInputCount() float64  { return (*s)[StatsArrayIndexS3IOInputCount] }  // unit: count
func (s *StatsArray) GetS3IOOutputCount() float64 { return (*s)[StatsArrayIndexS3IOOutputCount] } // unit: count
func (s *StatsArray) GetOutTrafficBytes() float64 { // unit: byte
	if s.GetVersion() < StatsArrayVersion2 {
		return 0
	}
	return (*s)[StatsArrayIndexOutTrafficBytes]
}
func (s *StatsArray) GetConnType() float64 {
	if s.GetVersion() < StatsArrayVersion3 {
		return 0
	}
	return (*s)[StatsArrayIndexConnType]
}

// WithVersion set the version array in StatsArray, please carefully to use.
func (s *StatsArray) WithVersion(v float64) *StatsArray { (*s)[StatsArrayIndexVersion] = v; return s }
func (s *StatsArray) WithTimeConsumed(v float64) *StatsArray {
	(*s)[StatsArrayIndexTimeConsumed] = v
	return s
}
func (s *StatsArray) WithMemorySize(v float64) *StatsArray {
	(*s)[StatsArrayIndexMemorySize] = v
	return s
}
func (s *StatsArray) WithS3IOInputCount(v float64) *StatsArray {
	(*s)[StatsArrayIndexS3IOInputCount] = v
	return s
}
func (s *StatsArray) WithS3IOOutputCount(v float64) *StatsArray {
	(*s)[StatsArrayIndexS3IOOutputCount] = v
	return s
}
func (s *StatsArray) WithOutTrafficBytes(v float64) *StatsArray {
	if s.GetVersion() >= StatsArrayVersion2 {
		(*s)[StatsArrayIndexOutTrafficBytes] = v
	}
	return s
}

func (s *StatsArray) WithConnType(v ConnType) *StatsArray {
	if s.GetVersion() >= StatsArrayVersion3 {
		(*s)[StatsArrayIndexConnType] = float64(v)
	}
	return s
}

func (s *StatsArray) ToJsonString() []byte {
	switch s.GetVersion() {
	case StatsArrayVersion1:
		return StatsArrayToJsonString((*s)[:StatsArrayLengthV1])
	case StatsArrayVersion2:
		return StatsArrayToJsonString((*s)[:StatsArrayLengthV2])
	case StatsArrayVersion3:
		return StatsArrayToJsonString((*s)[:StatsArrayLengthV3])
	default:
		return StatsArrayToJsonString((*s)[:])
	}
}

// Add do add two stats array together
// except for Element ConnType, which idx = StatsArrayIndexConnType, just keep s[StatsArrayIndexConnType] value.
func (s *StatsArray) Add(delta *StatsArray) *StatsArray {
	dstLen := len(*delta)
	if len(*s) < len(*delta) {
		dstLen = len(*s)
	}
	for idx := 1; idx < dstLen; idx++ {
		if idx == StatsArrayIndexConnType {
			continue
		}
		(*s)[idx] += (*delta)[idx]
	}
	return s
}

// StatsArrayToJsonString return json arr format
// example:
// [1,0,0,0,0] got `[1,0,0,0,0]`
// [1,2,3,4,5] got `[1,2,3.000,4,5]`
// [2,1,2,3,4,5] got `[2,3.000,4,5,6.000,7]`
func StatsArrayToJsonString(arr []float64) []byte {
	// len([1,184467440737095516161,18446744073709551616,18446744073709551616,18446744073709551616]") = 88
	buf := make([]byte, 0, 128)
	buf = append(buf, '[')
	for idx, v := range arr {
		if idx > 0 {
			buf = append(buf, ',')
		}
		if v == 0.0 {
			buf = append(buf, '0')
		} else if idx == StatsArrayIndexMemorySize {
			buf = strconv.AppendFloat(buf, v, 'f', Float64PrecForMemorySize, 64)
		} else {
			buf = strconv.AppendFloat(buf, v, 'f', 0, 64)
		}
	}
	buf = append(buf, ']')
	return buf
}

var initStatsArray = NewStatsArray()

var DefaultStatsArray = *initStatsArray.Init()

var DefaultStatsArrayJsonString = initStatsArray.Init().ToJsonString()

type statsInfoKey struct{}

// statistic info of sql
type StatsInfo struct {
	ParseDuration   time.Duration
	PlanDuration    time.Duration
	CompileDuration time.Duration

	PipelineTimeConsumption      time.Duration
	PipelineBlockTimeConsumption time.Duration

	S3AccessTimeConsumption time.Duration
	S3ReadBytes             uint
	S3WriteBytes            uint

	//temporary fields which is used to compute data above

	planStart time.Time

	compileStart time.Time
}

func (stats *StatsInfo) CompileStart() {
	if stats == nil {
		return
	}
	if !stats.compileStart.IsZero() {
		return
	}
	stats.compileStart = time.Now()
}

func (stats *StatsInfo) CompileEnd() {
	if stats == nil {
		return
	}
	stats.CompileDuration = time.Since(stats.compileStart)
}

func (stats *StatsInfo) PlanStart() {
	if stats == nil {
		return
	}
	stats.planStart = time.Now()
}

func (stats *StatsInfo) PlanEnd() {
	if stats == nil {
		return
	}
	stats.PlanDuration = time.Since(stats.planStart)
}

func ContextWithStatsInfo(requestCtx context.Context, stats *StatsInfo) context.Context {
	return context.WithValue(requestCtx, statsInfoKey{}, stats)
}

func StatsInfoFromContext(requestCtx context.Context) *StatsInfo {
	if requestCtx == nil {
		return nil
	}
	if stats, ok := requestCtx.Value(statsInfoKey{}).(*StatsInfo); ok {
		return stats
	}
	return nil
}

func EnsureStatsInfoCanBeFound(ctx context.Context, from context.Context) context.Context {
	if v := ctx.Value(statsInfoKey{}); v != nil {
		// already set
		return ctx
	}
	v := from.Value(statsInfoKey{})
	if v == nil {
		// not set in from
		return ctx
	}
	ctx = context.WithValue(ctx, statsInfoKey{}, v)
	return ctx
}
