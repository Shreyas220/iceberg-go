package puffin

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReaderAcceptsUnorderedBlobsInFooter(t *testing.T) {
	// Manually construct a file with two blobs, but listed in reverse order in the footer.

	blob1Data := []byte{1, 1, 1}
	blob2Data := []byte{2, 2, 2}

	// Layout: Magic | Blob1 | Blob2 | Footer | Trailer

	b := &bytes.Buffer{}
	b.Write(Magic[:])

	offset1 := int64(b.Len())
	b.Write(blob1Data)

	offset2 := int64(b.Len())
	b.Write(blob2Data)

	// Construct Footer with Blob2 listed BEFORE Blob1
	blobs := []BlobMetadata{
		{
			Type:       DeletionVectorV1,
			Offset:     offset2,
			Length:     int64(len(blob2Data)),
			Fields:     []int32{},
			SnapshotID: 1,
		},
		{
			Type:       DeletionVectorV1,
			Offset:     offset1,
			Length:     int64(len(blob1Data)),
			Fields:     []int32{},
			SnapshotID: 1,
		},
	}

	footer := Footer{Blobs: blobs}
	footerJSON, err := json.Marshal(footer)
	require.NoError(t, err)

	// Footer Start Magic
	b.Write(Magic[:])
	// Footer Payload
	b.Write(footerJSON)

	// Trailer
	trailer := make([]byte, footerTrailerSize)
	binary.LittleEndian.PutUint32(trailer[0:4], uint32(len(footerJSON)))
	binary.LittleEndian.PutUint32(trailer[4:8], 0) // Flags
	copy(trailer[8:], Magic[:])
	b.Write(trailer)

	fileBytes := b.Bytes()

	// Try to read it
	reader, err := NewPuffinReader(bytes.NewReader(fileBytes), int64(len(fileBytes)))
	require.NoError(t, err)

	_, err = reader.ReadFooter()
	// If the reader enforces strict order, this will fail.
	// If the Puffin spec allows arbitrary order, this SHOULD pass.
	// We want to know if it fails.
	if err != nil {
		t.Logf("Reader failed with: %v", err)
	} else {
		t.Log("Reader accepted unordered blobs")
	}
}
