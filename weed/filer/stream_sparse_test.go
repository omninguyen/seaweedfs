package filer

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/url"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

// A sparse file has ranges no chunk covers: a write past the end through
// weed mount, or a size extended with truncate. Those ranges read as zeros,
// as on POSIX and as ChunkReadAt and StreamContent already read them, and the
// file is FileSize(entry) bytes long. ChunkStreamReader backs NewFileReader
// (replication sinks, sftp, the Iceberg worker) and the chunk merges of the
// filer and of weed mount, so it must return the same bytes and must not end
// the stream early without an error.

type sparseChunk struct {
	fileId string
	offset int64
	data   []byte
}

// sparseLayout serves each chunk from a test volume server and returns the
// chunks, a lookup for them and the file content they describe up to size:
// zeros wherever no chunk is written.
func sparseLayout(t *testing.T, size int64, chunks ...sparseChunk) ([]*filer_pb.FileChunk, *testMasterClient, []byte) {
	t.Helper()
	served := make(map[string][]byte, len(chunks))
	for _, c := range chunks {
		served[c.fileId] = c.data
	}
	server := createTestServer(served)
	t.Cleanup(server.Close)

	master := &testMasterClient{urls: make(map[string][]string, len(chunks))}
	want := make([]byte, size)
	fileChunks := make([]*filer_pb.FileChunk, 0, len(chunks))
	for i, c := range chunks {
		master.urls[c.fileId] = []string{server.URL + "/" + c.fileId}
		copy(want[c.offset:], c.data)
		fileChunks = append(fileChunks, &filer_pb.FileChunk{
			FileId:       c.fileId,
			Offset:       c.offset,
			Size:         uint64(len(c.data)),
			ModifiedTsNs: int64(i + 1),
		})
	}
	return fileChunks, master, want
}

// holeBetweenChunks is a 30-byte file: "a" x10, a 10-byte hole, "b" x10.
func holeBetweenChunks(t *testing.T) ([]*filer_pb.FileChunk, *testMasterClient, []byte) {
	return sparseLayout(t, 30,
		sparseChunk{"1,0a", 0, bytes.Repeat([]byte("a"), 10)},
		sparseChunk{"2,0b", 20, bytes.Repeat([]byte("b"), 10)},
	)
}

func newSparseReader(t *testing.T, chunks []*filer_pb.FileChunk, master *testMasterClient) *ChunkStreamReader {
	t.Helper()
	reader := NewChunkStreamReaderFromLookup(context.Background(), master.GetLookupFileIdFunction(), chunks)
	t.Cleanup(func() { _ = reader.Close() })
	return reader
}

// requireWholeFile reads the stream to its end. A short read with no error is
// the data loss these tests look for, so the length is checked on its own.
func requireWholeFile(t *testing.T, reader io.Reader, want []byte) {
	t.Helper()
	got, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.NoError(t, ReaderSourceError(reader))
	require.Equal(t, len(want), len(got), "the stream ended early without an error: %d of %d bytes", len(got), len(want))
	require.Equal(t, want, got)
}

// TestChunkStreamReaderReadsHoleBetweenChunksAsZeros: the hole reads as zeros
// and the chunk after it is still read.
func TestChunkStreamReaderReadsHoleBetweenChunksAsZeros(t *testing.T) {
	chunks, master, want := holeBetweenChunks(t)
	requireWholeFile(t, newSparseReader(t, chunks, master), want)
}

// TestChunkStreamReaderReadsLeadingHoleAsZeros: a file whose first write was
// past offset 0 starts with zeros instead of reading as empty.
func TestChunkStreamReaderReadsLeadingHoleAsZeros(t *testing.T) {
	chunks, master, want := sparseLayout(t, 20,
		sparseChunk{"1,0b", 10, bytes.Repeat([]byte("b"), 10)},
	)
	requireWholeFile(t, newSparseReader(t, chunks, master), want)
}

// TestChunkStreamReaderReadsEveryHoleOfAManyHoleFile: data after every hole is
// kept, not only after the first one.
func TestChunkStreamReaderReadsEveryHoleOfAManyHoleFile(t *testing.T) {
	var chunks []sparseChunk
	for i := 0; i < 8; i++ {
		chunks = append(chunks, sparseChunk{
			fileId: fmt.Sprintf("%d,%02x", i+1, i),
			offset: int64(i) * 4096,
			data:   bytes.Repeat([]byte{byte('a' + i)}, 1024),
		})
	}
	fileChunks, master, want := sparseLayout(t, 7*4096+1024, chunks...)
	requireWholeFile(t, newSparseReader(t, fileChunks, master), want)
}

// TestChunkStreamReaderReadAtInsideHole: ReadAt in a hole answers zeros. A
// part reader (io.SectionReader) starting in a hole depends on it.
func TestChunkStreamReaderReadAtInsideHole(t *testing.T) {
	chunks, master, want := holeBetweenChunks(t)
	reader := newSparseReader(t, chunks, master)

	buf := bytes.Repeat([]byte{0xff}, 5)
	n, err := reader.ReadAt(buf, 12)
	require.NoError(t, err)
	require.Equal(t, 5, n)
	require.Equal(t, want[12:17], buf)
}

// TestChunkStreamReaderReadAtAcrossHole: one ReadAt spanning chunk, hole and
// chunk fills the whole buffer.
func TestChunkStreamReaderReadAtAcrossHole(t *testing.T) {
	chunks, master, want := holeBetweenChunks(t)
	reader := newSparseReader(t, chunks, master)

	buf := make([]byte, 20)
	n, err := reader.ReadAt(buf, 5)
	require.NoError(t, err)
	require.Equal(t, 20, n)
	require.Equal(t, want[5:25], buf)
}

// TestChunkStreamReaderSectionReadsMatchTheFile reads the file in parts
// through io.SectionReader, as the S3 sink's multipart upload does, and joins
// them back.
func TestChunkStreamReaderSectionReadsMatchTheFile(t *testing.T) {
	chunks, master, want := holeBetweenChunks(t)
	reader := newSparseReader(t, chunks, master)

	var joined []byte
	for off := int64(0); off < int64(len(want)); off += 8 {
		part, err := io.ReadAll(io.NewSectionReader(reader, off, 8))
		require.NoError(t, err, "part at %d", off)
		joined = append(joined, part...)
	}
	require.Equal(t, want, joined)
}

// TestChunkStreamReaderSeekCountsHoles: the stream is as long as the file, so
// SeekEnd lands at its end and an offset in the last chunk can be sought.
func TestChunkStreamReaderSeekCountsHoles(t *testing.T) {
	chunks, master, want := holeBetweenChunks(t)
	reader := newSparseReader(t, chunks, master)

	end, err := reader.Seek(0, io.SeekEnd)
	require.NoError(t, err)
	require.Equal(t, int64(len(want)), end)

	pos, err := reader.Seek(25, io.SeekStart)
	require.NoError(t, err)
	require.Equal(t, int64(25), pos)
	rest, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, want[25:], rest)
}

// TestChunkStreamReaderMatchesStreamContent: for the same chunks the reader
// returns what StreamContent, the filer's HTTP read path, writes.
func TestChunkStreamReaderMatchesStreamContent(t *testing.T) {
	layouts := map[string][]sparseChunk{
		"hole between chunks": {
			{"1,0a", 0, bytes.Repeat([]byte("a"), 100)},
			{"2,0b", 300, bytes.Repeat([]byte("b"), 100)},
		},
		"leading hole": {
			{"1,0a", 64, bytes.Repeat([]byte("a"), 100)},
		},
		"overwrite next to a hole": {
			{"1,0a", 0, bytes.Repeat([]byte("a"), 100)},
			{"2,0b", 50, bytes.Repeat([]byte("b"), 20)},
			{"3,0c", 200, bytes.Repeat([]byte("c"), 30)},
		},
	}
	for name, layout := range layouts {
		t.Run(name, func(t *testing.T) {
			var size int64
			for _, c := range layout {
				size = max(size, c.offset+int64(len(c.data)))
			}
			chunks, master, want := sparseLayout(t, size, layout...)

			var streamed bytes.Buffer
			require.NoError(t, StreamContent(master, &streamed, chunks, 0, size))
			require.Equal(t, want, streamed.Bytes(), "StreamContent is the reference")

			requireWholeFile(t, newSparseReader(t, chunks, master), streamed.Bytes())
		})
	}
}

// sparseFilerClient answers LookupVolume for the test volume server, so
// NewFileReader can be used the way replication sinks and sftp use it.
type sparseFilerClient struct {
	volumeServer string
}

type sparseVolumeLookup struct {
	filer_pb.SeaweedFilerClient
	volumeServer string
}

func (l sparseVolumeLookup) LookupVolume(_ context.Context, in *filer_pb.LookupVolumeRequest, _ ...grpc.CallOption) (*filer_pb.LookupVolumeResponse, error) {
	resp := &filer_pb.LookupVolumeResponse{LocationsMap: map[string]*filer_pb.Locations{}}
	for _, vid := range in.VolumeIds {
		resp.LocationsMap[vid] = &filer_pb.Locations{Locations: []*filer_pb.Location{{Url: l.volumeServer}}}
	}
	return resp, nil
}

func (c sparseFilerClient) WithFilerClient(_ bool, fn func(filer_pb.SeaweedFilerClient) error) error {
	return fn(sparseVolumeLookup{volumeServer: c.volumeServer})
}
func (c sparseFilerClient) AdjustedUrl(location *filer_pb.Location) string { return location.Url }
func (c sparseFilerClient) GetDataCenter() string                          { return "" }

func sparseFilerClientFor(t *testing.T, master *testMasterClient) sparseFilerClient {
	t.Helper()
	for _, urls := range master.urls {
		u, err := url.Parse(urls[0])
		require.NoError(t, err)
		return sparseFilerClient{volumeServer: u.Host}
	}
	t.Fatal("no chunk is served")
	return sparseFilerClient{}
}

// TestNewFileReaderReadsWholeSparseEntry: NewFileReader returns FileSize(entry)
// bytes, including a hole between chunks and one at the end, such as left by
// truncate -s extending the file.
func TestNewFileReaderReadsWholeSparseEntry(t *testing.T) {
	cases := map[string]struct {
		fileSize uint64
		layout   []sparseChunk
	}{
		"hole between chunks": {30, []sparseChunk{
			{"1,0a", 0, bytes.Repeat([]byte("a"), 10)},
			{"2,0b", 20, bytes.Repeat([]byte("b"), 10)},
		}},
		"trailing hole": {40, []sparseChunk{
			{"1,0a", 0, bytes.Repeat([]byte("a"), 10)},
		}},
		"both": {50, []sparseChunk{
			{"1,0a", 0, bytes.Repeat([]byte("a"), 10)},
			{"2,0b", 20, bytes.Repeat([]byte("b"), 10)},
		}},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			chunks, master, want := sparseLayout(t, int64(tc.fileSize), tc.layout...)
			entry := &filer_pb.Entry{
				Name:       "sparse",
				Chunks:     chunks,
				Attributes: &filer_pb.FuseAttributes{FileSize: tc.fileSize},
			}
			require.Equal(t, tc.fileSize, FileSize(entry))

			requireWholeFile(t, NewFileReader(sparseFilerClientFor(t, master), entry), want)
		})
	}
}
