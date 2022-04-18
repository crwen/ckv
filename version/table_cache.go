package version

import "SimpleKV/sstable"

func (vs *VersionSet) GetIndex(fid uint64) *sstable.IndexBlock {
	return vs.tableCache.GetIndex(fid)
}
func (vs *VersionSet) AddIndex(fid uint64, index *sstable.IndexBlock) {
	vs.tableCache.AddIndex(fid, index)
}
