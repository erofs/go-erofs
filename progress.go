package erofs

import "sync/atomic"

// Progress reports how much work a [Writer] has done. It is returned by
// [Writer.Progress]. The counts show that work is being done; they are not
// an estimate of the final image size or of the time left.
type Progress struct {
	// Entries is the number of entries in the image, excluding the root
	// directory. It goes down when entries are removed.
	Entries int64

	// EntriesWritten is the number of entries Close has written. It equals
	// Entries once Close has finished, and not before.
	EntriesWritten int64

	// DataBytes is the number of file data bytes Close will write: data
	// taken in through [File.Write], or by reference from a
	// [Writer.CopyFrom] source, for files still in the image. It goes down
	// when the last name of a file is removed or the file is replaced.
	// Files added by a [MetadataOnly] CopyFrom are not counted.
	DataBytes int64

	// DataBytesWritten is the number of file data bytes Close has written.
	// Data stored in the data file set with [WithDataFile] counts as
	// written when Close writes the file's chunk indexes. It equals
	// DataBytes once Close has finished; use EntriesWritten to tell when
	// Close has finished, as DataBytesWritten can reach DataBytes before
	// the metadata is written.
	DataBytesWritten int64
}

// progressCounters holds the counters behind [Writer.Progress]. The Writer
// updates them atomically so Progress may be called from any goroutine.
type progressCounters struct {
	entries          atomic.Int64
	entriesWritten   atomic.Int64
	dataBytes        atomic.Int64
	dataBytesWritten atomic.Int64
}

// addData counts n bytes of file data taken in for ino. Data for an inode
// with no names left, such as a file removed while still open, is not
// counted because Close will not write it.
func (fsys *Writer) addData(ino *fsInode, n int64) {
	if ino.names > 0 && n > 0 {
		ino.dataBytes += n
		fsys.progress.dataBytes.Add(n)
	}
}

// dropName records that one name referring to ino has left the tree. When
// the last name goes, the inode's data is no longer counted.
func (fsys *Writer) dropName(ino *fsInode) {
	ino.names--
	if ino.names == 0 && ino.dataBytes > 0 {
		fsys.progress.dataBytes.Add(-ino.dataBytes)
		ino.dataBytes = 0
	}
}

// Progress returns the Writer's current progress. It may be called from any
// goroutine, including while entries are being added or Close runs.
func (fsys *Writer) Progress() Progress {
	p := &fsys.progress
	return Progress{
		Entries:          p.entries.Load(),
		EntriesWritten:   p.entriesWritten.Load(),
		DataBytes:        p.dataBytes.Load(),
		DataBytesWritten: p.dataBytesWritten.Load(),
	}
}
