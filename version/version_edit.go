package version

import "ckv/sstable"

type VersionEdit struct {
	// logNumber      uint64
	// logNumber      uint64
	// prevFileNumber uint64
	// nextFileNumber uint64

	deletes  []*TableMeta
	adds     []*TableMeta
	vdeletes []*tableState
	vadds    []*tableState
}

type TableMeta struct {
	f     *FileMetaData
	level int
}

func NewVersionEdit() *VersionEdit {
	return &VersionEdit{
		deletes: make([]*TableMeta, 0),
		adds:    make([]*TableMeta, 0),
	}
}

func (ve *VersionEdit) RecordAddFileMeta(level int, t *sstable.Table) {
	fm := &FileMetaData{
		id:       t.Fid(),
		largest:  t.MaxKey,
		smallest: t.MinKey,
		fileSize: t.Size(),
	}
	ve.adds = append(ve.adds, &TableMeta{f: fm, level: level})
}

func (ve *VersionEdit) RecordDeleteFileMeta(level int, t *sstable.Table) {
	fm := &FileMetaData{
		id:       t.Fid(),
		largest:  t.MaxKey,
		smallest: t.MinKey,
		fileSize: t.Size(),
	}
	ve.deletes = append(ve.deletes, &TableMeta{f: fm, level: level})
}

func (ve *VersionEdit) DeleteFileMetas(level int, tables []*sstable.Table) {
	for _, table := range tables {
		ve.RecordDeleteFileMeta(level, table)
	}
}
