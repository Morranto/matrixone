package rightsemi

import (
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	Build = iota
	Probe
	End
)

type evalVector struct {
	needFree bool
	vec      *vector.Vector
}

type container struct {
	state int

	inBuckets []uint8

	bat *batch.Batch

	evecs []evalVector
	vecs  []*vector.Vector

	mp *hashmap.JoinMap

	matchedSels map[int64]bool
}

type Argument struct {
	ctr        *container
	Ibucket    uint64 // index in buckets
	Nbucket    uint64 // buckets count
	Result     []int32
	Typs       []types.Type
	Cond       *plan.Expr
	Conditions [][]*plan.Expr
}

func (arg *Argument) Free(proc *process.Process, pipelineFailed bool) {
	ctr := arg.ctr
	if ctr != nil {
		mp := proc.Mp()
		ctr.cleanBatch(mp)
		ctr.cleanEvalVectors(mp)
		ctr.cleanHashMap()
	}
}

func (ctr *container) cleanBatch(mp *mpool.MPool) {
	if ctr.bat != nil {
		ctr.bat.Clean(mp)
		ctr.bat = nil
	}
}

func (ctr *container) cleanHashMap() {
	if ctr.mp != nil {
		ctr.mp.Free()
		ctr.mp = nil
	}
}

func (ctr *container) cleanEvalVectors(mp *mpool.MPool) {
	for i := range ctr.evecs {
		if ctr.evecs[i].needFree && ctr.evecs[i].vec != nil {
			ctr.evecs[i].vec.Free(mp)
			ctr.evecs[i].vec = nil
		}
	}
}
