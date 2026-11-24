package filer

import (
	"bytes"
	"io"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/stretchr/testify/require"
)

// These pin what ChunkStreamReader and NewFileReader return on master for a
// file without holes, and for an entry whose size names content kept in remote
// storage, so reading holes as zeros changes nothing else. They pass with and
// without that change.

// denseLayout is a 30-byte file in three adjacent chunks.
func denseLayout(t *testing.T) ([]*filer_pb.FileChunk, *testMasterClient, []byte) {
	return sparseLayout(t, 30,
		sparseChunk{"1,0a", 0, bytes.Repeat([]byte("a"), 10)},
		sparseChunk{"2,0b", 10, bytes.Repeat([]byte("b"), 10)},
		sparseChunk{"3,0c", 20, bytes.Repeat([]byte("c"), 10)},
	)
}

// TestChunkStreamReaderReadsAdjacentChunks: the whole file, its size and
// ReadAt inside and across chunks read as before.
func TestChunkStreamReaderReadsAdjacentChunks(t *testing.T) {
	chunks, master, want := denseLayout(t)
	requireWholeFile(t, newSparseReader(t, chunks, master), want)

	reader := newSparseReader(t, chunks, master)
	end, err := reader.Seek(0, io.SeekEnd)
	require.NoError(t, err)
	require.Equal(t, int64(30), end)

	buf := make([]byte, 6)
	n, err := reader.ReadAt(buf, 2)
	require.NoError(t, err)
	require.Equal(t, 6, n)
	require.Equal(t, want[2:8], buf)

	n, err = reader.ReadAt(buf, 17)
	require.NoError(t, err)
	require.Equal(t, 6, n)
	require.Equal(t, want[17:23], buf)
}

// TestChunkStreamReaderEndOfFile: a read at or past the end answers io.EOF and
// a seek past the end is refused.
func TestChunkStreamReaderEndOfFile(t *testing.T) {
	chunks, master, want := denseLayout(t)
	reader := newSparseReader(t, chunks, master)

	buf := make([]byte, 8)
	n, err := reader.ReadAt(buf, 26)
	require.ErrorIs(t, err, io.EOF)
	require.Equal(t, 4, n)
	require.Equal(t, want[26:], buf[:n])

	n, err = reader.ReadAt(buf, 30)
	require.ErrorIs(t, err, io.EOF)
	require.Zero(t, n)
	n, err = reader.ReadAt(buf, 31)
	require.ErrorIs(t, err, io.EOF)
	require.Zero(t, n)

	_, err = reader.Seek(31, io.SeekStart)
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)

	pos, err := reader.Seek(30, io.SeekStart)
	require.NoError(t, err)
	require.Equal(t, int64(30), pos)
	n, err = reader.Read(buf)
	require.ErrorIs(t, err, io.EOF)
	require.Zero(t, n)
}

// TestChunkStreamReaderReadsOverwrites: where a later chunk overwrites part of
// an earlier one, the later bytes win and nothing reads as a hole.
func TestChunkStreamReaderReadsOverwrites(t *testing.T) {
	chunks, master, want := sparseLayout(t, 30,
		sparseChunk{"1,0a", 0, bytes.Repeat([]byte("a"), 30)},
		sparseChunk{"2,0b", 5, bytes.Repeat([]byte("b"), 10)},
		sparseChunk{"3,0c", 20, bytes.Repeat([]byte("c"), 10)},
	)
	require.NotContains(t, string(want), "\x00")

	var streamed bytes.Buffer
	require.NoError(t, StreamContent(master, &streamed, chunks, 0, 30))
	require.Equal(t, want, streamed.Bytes())
	requireWholeFile(t, newSparseReader(t, chunks, master), want)
}

// TestNewFileReaderReadsDenseEntry: an entry whose size is where its chunks
// end reads as its chunks.
func TestNewFileReaderReadsDenseEntry(t *testing.T) {
	chunks, master, want := denseLayout(t)
	entry := &filer_pb.Entry{Name: "dense", Chunks: chunks, Attributes: &filer_pb.FuseAttributes{FileSize: 30}}
	requireWholeFile(t, NewFileReader(sparseFilerClientFor(t, master), entry), want)
}

// TestNewFileReaderKeepsRemoteEntryToItsChunks: a remote-storage entry's size
// counts content the local chunks may not hold, so the reader returns the
// chunks only and does not invent zeros for the rest.
func TestNewFileReaderKeepsRemoteEntryToItsChunks(t *testing.T) {
	chunks, master, want := sparseLayout(t, 10,
		sparseChunk{"1,0a", 0, bytes.Repeat([]byte("a"), 10)},
	)
	entry := &filer_pb.Entry{
		Name:        "remote",
		Chunks:      chunks,
		Attributes:  &filer_pb.FuseAttributes{FileSize: 40},
		RemoteEntry: &filer_pb.RemoteEntry{RemoteSize: 40},
	}
	requireWholeFile(t, NewFileReader(sparseFilerClientFor(t, master), entry), want)
}

// TestNewFileReaderReadsInlineContent: inline content is returned as stored.
func TestNewFileReaderReadsInlineContent(t *testing.T) {
	entry := &filer_pb.Entry{Name: "inline", Content: []byte("inline"), Attributes: &filer_pb.FuseAttributes{FileSize: 6}}
	got, err := io.ReadAll(NewFileReader(nil, entry))
	require.NoError(t, err)
	require.Equal(t, "inline", string(got))
}

// TestNewFileReaderFailsOnAGoneChunk: a chunk the volume no longer has fails
// the read; the entry's size does not turn the failure into zeros.
func TestNewFileReaderFailsOnAGoneChunk(t *testing.T) {
	_, master, _ := sparseLayout(t, 10, sparseChunk{"1,0a", 0, bytes.Repeat([]byte("a"), 10)})
	entry := &filer_pb.Entry{
		Name:       "gone",
		Chunks:     []*filer_pb.FileChunk{{FileId: "7,01637037d6", Size: 8}},
		Attributes: &filer_pb.FuseAttributes{FileSize: 40},
	}
	reader := NewFileReader(sparseFilerClientFor(t, master), entry)
	_, err := io.ReadAll(reader)
	require.Error(t, err)
	require.Error(t, ReaderSourceError(reader))
}
