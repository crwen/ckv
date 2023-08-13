package version

import (
	"ckv/file"
	"ckv/sstable"
	"ckv/utils"
	"ckv/utils/convert"
	"ckv/vlog"
	"fmt"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"time"
)

func (vs *VersionSet) RunGC() {
	randomDelay := time.NewTimer(time.Duration(rand.Int31n(2000)) * time.Millisecond)

	<-randomDelay.C

	ticker := time.NewTicker(5000 * time.Millisecond)
	defer ticker.Stop()
	for {
		<-ticker.C
		vs.mergeVLog()
	}
}

func (vs *VersionSet) mergeVLog() {

	vs.lock.Lock()
	defer vs.lock.Unlock()

	fids, fid := vs.info.PickGroupForMerge()
	if len(fids) == 1 {
		return
	}
	log.Println(fid, fids)
	vs.info.PrintVLogGroup()
	filter := func(files []uint64) []uint64 {
		res := make([]uint64, 0)
		for i := range files {
			fid := files[i]
			//if state, ok := vs.info.GetVTableState(fid); ok && state == NORMAL {
			res = append(res, fid)
			//vs.info.SetTableState(fid, GC)
			//}
		}
		return res
	}
	if state, ok := vs.info.GetTableState(fid); ok && state == NORMAL {
		mergeFids := filter(fids)
		// check whether there is any sst being compacted
		if len(mergeFids) == len(fids) {
			// set vlog state as GC
			for i := range mergeFids {
				vs.info.SetVTableState(mergeFids[i], GC)
			}
			// set sst state as GC, it can't be selected to be compacted
			vs.info.SetTableState(fid, GC)

			for i := range vs.current.files {
				for j := range vs.current.files[i] {
					if vs.current.files[i][j].id == fid {
						vs.pendingGC = &VFileMetaData{
							sstId:    fid,
							largest:  vs.current.files[i][j].largest,
							smallest: vs.current.files[i][j].smallest,
							level:    i,
						}
						break
					}
				}
			}

			go vs.mergeVLogs(fid, mergeFids)
		}
	}

}

// mergeVLogs merge vlogs that the ssTable refs
func (vs *VersionSet) mergeVLogs(sstFid uint64, fids []uint64) (*vlog.VLogFile, error) {
	opt := vs.current.opt
	table := vs.FindTable(sstFid)
	iter := table.NewIterator(opt)

	newFid := vs.IncreaseNextFileNumber(1)
	sstName := file.FileNameSSTable(opt.WorkDir, newFid)

	newVLog := openVLog(opt, newFid)
	vlogs := make(map[uint64]*vlog.VLogFile)
	//defer func() {
	//	for _, v := range vlogs {
	//		v.Close()
	//	}
	//}()

	builder := sstable.NewTableBuiler(opt)
	var entry *utils.Entry
	// merge vlogs
	for iter.Rewind(); iter.Valid(); iter.Next() {
		e := iter.Item().Entry()
		entry = iter.Item().Entry()
		if e.Value[0] == utils.VAL_PTR {
			fid := convert.BytesToU64(e.Value[1:])
			pos := convert.BytesToU32(e.Value[9:])
			var vlog *vlog.VLogFile
			if v, ok := vlogs[fid]; !ok {
				vlog = openVLog(opt, fid)
				vlogs[fid] = vlog
			} else {
				vlog = v
			}
			data, _, err := vlog.ReadRecordBytes(pos)
			if err != nil {
				return nil, err
			}
			writeAt := newVLog.Pos()
			newVLog.WriteData(data)

			val := make([]byte, 13)
			val[1] = utils.VAL_PTR
			copy(val[1:], convert.U64ToBytes(newFid))
			copy(val[9:], convert.U32ToBytes(writeAt))

			e.Value = val
		}
		builder.Add(e, false)
	}
	iter.Close()

	t, err := builder.Flush(sstName)
	if err != nil {
		return nil, err
	}
	t.MaxKey = entry.Key

	log.Printf("GC for SSTable %d. Delete %d vlog files. Create new SSTable %d \n",
		sstFid, len(fids), newFid)

	vs.lock.Lock()
	defer vs.lock.Unlock()

	// write manifest
	ve := NewVersionEdit()
	ve.RecordAddFileMeta(vs.pendingGC.level, t)
	ve.DeleteFileMetas(vs.pendingGC.level, []*sstable.Table{table})
	vs.LogAndApply(ve)

	// write new meta
	vs.addFileMeta(vs.pendingGC.level, t)

	//vs.info.MergeVLogGroup(mergeFids, newFid)
	vs.info.AddNewVLogWithGroup(newFid)
	vs.info.SetTableState(newFid, NORMAL)

	// delete old meta
	vs.DeleteFileMeta(vs.pendingGC.level, vs.pendingGC.level, table)
	vs.info.SetTableState(sstFid, NORMAL)
	table.DecrRef(func() error {
		for i := range fids {
			vlogName := file.FileNameVLog(opt.WorkDir, fids[i])
			os.Remove(vlogName)
		}
		return nil
	})

	vs.info.RemoveVLogFromGroup(fids)

	vs.pendingGC = nil
	return newVLog, nil
}

func openVLog(opt *utils.Options, fid uint64) *vlog.VLogFile {
	fileOpt := &file.Options{
		FID:      fid,
		FileName: filepath.Join(opt.WorkDir, fmt.Sprintf("%05d%s", fid, utils.VLOG_FILE_EXT)),
		Dir:      opt.WorkDir,
		Flag:     os.O_CREATE | os.O_RDWR,
		MaxSz:    int(opt.MemTableSize),
	}
	return vlog.OpenVLogFile(fileOpt)
}
