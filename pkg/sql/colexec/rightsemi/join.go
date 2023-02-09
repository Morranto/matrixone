package rightsemi

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func String(_ any, buf *bytes.Buffer) {
	buf.WriteString(" right semi join ")
}

func Prepare(proc *process.Process, arg any) error {
	ap := arg.(*Argument)
	ap.ctr = new(container)
	ap.ctr.inBuckets = make([]uint8, hashmap.UnitLimit)
	ap.ctr.evecs = make([]evalVector, len(ap.Conditions[0]))
	ap.ctr.vecs = make([]*vector.Vector, len(ap.Conditions[0]))
	ap.ctr.matchedSels = make(map[int64]bool)
	return nil
}

func Call(idx int, proc *process.Process, arg any, isFirst bool, isLast bool) (bool, error) {
	anal := proc.GetAnalyze(idx)
	anal.Start()
	defer anal.Stop()
	ap := arg.(*Argument)
	ctr := ap.ctr
	for {
		switch ctr.state {
		case Build:
			if err := ctr.build(ap, proc, anal); err != nil {
				ap.Free(proc, true)
				return false, err
			}
			ctr.state = Probe

		case Probe:
			bat := <-proc.Reg.MergeReceivers[0].Ch
			if bat == nil {
				ctr.state = End
				continue
			}
			if bat.Length() == 0 {
				continue
			}
			if ctr.bat == nil || ctr.bat.Length() == 0 {
				bat.Clean(proc.Mp())
				continue
			}
			if err := ctr.probe(bat, ap, proc, anal, isFirst, isLast); err != nil {
				ap.Free(proc, true)
				return false, err
			}

		default:
			if ctr.bat == nil || ctr.bat.Length() == 0 {
				proc.SetInputBatch(nil)
			} else {
				ctr.probe_finish(ap, proc, anal, isFirst, isLast)
			}
			ap.Free(proc, false)
			return true, nil
		}
	}
}

func (ctr *container) build(ap *Argument, proc *process.Process, anal process.Analyze) error {
	bat := <-proc.Reg.MergeReceivers[1].Ch
	if bat != nil {
		ctr.bat = bat
		ctr.mp = bat.Ht.(*hashmap.JoinMap).Dup()
	}
	return nil
}

func (ctr *container) probe(bat *batch.Batch, ap *Argument, proc *process.Process, anal process.Analyze, isFirst bool, isLast bool) error {
	defer bat.Clean(proc.Mp())
	anal.Input(bat, isFirst)

	ctr.cleanEvalVectors(proc.Mp())
	if err := ctr.evalJoinCondition(bat, ap.Conditions[0], proc); err != nil {
		return err
	}
	count := bat.Length()
	mSels := ctr.mp.Sels()
	itr := ctr.mp.Map().NewIterator()
	for i := 0; i < count; i += hashmap.UnitLimit {
		n := count - i
		if n > hashmap.UnitLimit {
			n = hashmap.UnitLimit
		}
		copy(ctr.inBuckets, hashmap.OneUInt8s)
		vals, zvals := itr.Find(i, n, ctr.vecs, ctr.inBuckets)
		for k := 0; k < n; k++ {
			if ctr.inBuckets[k] == 0 || zvals[k] == 0 || vals[k] == 0 {
				continue
			}
			sels := mSels[vals[k]-1]
			if ap.Cond != nil {
				for _, sel := range sels {
					vec, err := colexec.JoinFilterEvalExprInBucket(bat, ctr.bat, i+k, int(sel), proc, ap.Cond)
					if err != nil {
						return err
					}
					bs := vec.Col.([]bool)
					if bs[0] {
						ctr.matchedSels[sel] = true
					}
					vec.Free(proc.Mp())
				}
			} else {
				for _, sel := range sels {
					ctr.matchedSels[int64(sel)] = true
				}
			}
		}
	}
	return nil
}

func (ctr *container) probe_finish(ap *Argument, proc *process.Process, anal process.Analyze, isFirst bool, isLast bool) error {

	rbat := batch.NewWithSize(len(ap.Result))
	rbat.Zs = proc.Mp().GetSels()
	for i, pos := range ap.Result {
		rbat.Vecs[i] = vector.New(ctr.bat.Vecs[pos].Typ)
	}
	matchedSels := make([]int64, 0)

	for sel := range ctr.matchedSels {
		matchedSels = append(matchedSels, sel)
		rbat.Zs = append(rbat.Zs, ctr.bat.Zs[sel])
	}

	for i := range ctr.bat.Vecs {
		if err := vector.Union(rbat.Vecs[i], ctr.bat.Vecs[i], matchedSels, true, proc.Mp()); err != nil {
			rbat.Clean(proc.Mp())
			return err
		}
	}
	rbat.ExpandNulls()
	anal.Output(rbat, isLast)
	proc.SetInputBatch(rbat)

	return nil
}

func (ctr *container) evalJoinCondition(bat *batch.Batch, conds []*plan.Expr, proc *process.Process) error {
	for i, cond := range conds {
		vec, err := colexec.EvalExpr(bat, proc, cond)
		if err != nil || vec.ConstExpand(false, proc.Mp()) == nil {
			ctr.cleanEvalVectors(proc.Mp())
			return err
		}
		ctr.vecs[i] = vec
		ctr.evecs[i].vec = vec
		ctr.evecs[i].needFree = true
		for j := range bat.Vecs {
			if bat.Vecs[j] == vec {
				ctr.evecs[i].needFree = false
				break
			}
		}
	}
	return nil
}
