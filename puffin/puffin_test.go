// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package puffin

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRoundTripLayout(t *testing.T) {
	b := &bytes.Buffer{}
	w, err := NewWriter(b)
	require.NoError(t, err)

	blobData := []byte{1, 2, 3, 4}
	meta := BlobMetadata{Type: DeletionVectorV1, SnapshotID: 1, SequenceNumber: 1}
	require.NoError(t, w.AddBlob(meta, blobData))
	require.NoError(t, w.SetProperties(map[string]string{"k": "v"}))
	require.NoError(t, w.SetCreatedBy("tester"))
	require.NoError(t, w.Finish())

	fileBytes := b.Bytes()
	reader, err := NewReader(bytes.NewReader(fileBytes), int64(len(fileBytes)))
	require.NoError(t, err)
	footer, err := reader.ReadFooter()
	require.NoError(t, err)
	require.Len(t, footer.Blobs, 1)
	require.Equal(t, "v", footer.Properties["k"])
	require.Equal(t, "tester", footer.Properties[CreatedBy])

	readBlob, err := reader.ReadBlob(footer.Blobs[0])
	require.NoError(t, err)
	require.Equal(t, blobData, readBlob)

	// Layout: [Magic][Blob][Magic][FooterJSON][Len][Flags][Magic]
	pos := 0
	require.Equal(t, Magic[:], fileBytes[pos:pos+MagicSize])
	pos += MagicSize
	pos += len(blobData)
	require.Equal(t, Magic[:], fileBytes[pos:pos+MagicSize])
	pos += MagicSize
	footerPayloadLen := binary.LittleEndian.Uint32(fileBytes[len(fileBytes)-footerTrailerSize : len(fileBytes)-footerTrailerSize+4])
	flags := binary.LittleEndian.Uint32(fileBytes[len(fileBytes)-footerTrailerSize+4 : len(fileBytes)-footerTrailerSize+8])
	require.Equal(t, uint32(0), flags)
	require.Equal(t, int(footerPayloadLen), len(fileBytes)-pos-footerTrailerSize)
	require.Equal(t, Magic[:], fileBytes[len(fileBytes)-MagicSize:])
}

func TestFooterStartMagicRequired(t *testing.T) {
	file := buildFile(t, buildOptions{corruptFooterStartMagic: true})
	reader, err := NewReader(bytes.NewReader(file), int64(len(file)))
	require.NoError(t, err)
	_, err = reader.ReadFooter()
	require.ErrorContains(t, err, "invalid footer start magic")
}

func TestTrailingMagicRequired(t *testing.T) {
	file := buildFile(t, buildOptions{corruptTrailingMagic: true})
	_, err := NewReader(bytes.NewReader(file), int64(len(file)))
	require.ErrorContains(t, err, "invalid trailing magic")
}

func TestCompressedFooterFlagRejected(t *testing.T) {
	file := buildFile(t, buildOptions{footerFlags: 1})
	reader, err := NewReader(bytes.NewReader(file), int64(len(file)))
	require.NoError(t, err)
	_, err = reader.ReadFooter()
	require.ErrorContains(t, err, "compressed footer unsupported")
}

func TestUnknownFooterFlagRejected(t *testing.T) {
	file := buildFile(t, buildOptions{footerFlags: 0x2})
	reader, err := NewReader(bytes.NewReader(file), int64(len(file)))
	require.NoError(t, err)
	_, err = reader.ReadFooter()
	require.ErrorContains(t, err, "unknown footer flags")
}

func TestFooterLenTooLarge(t *testing.T) {
	file := buildFile(t, buildOptions{footerLenOverride: 0xFFFFFFFF})
	reader, err := NewReader(bytes.NewReader(file), int64(len(file)))
	require.NoError(t, err)
	_, err = reader.ReadFooter()
	require.Error(t, err)
}

func TestFooterSizeOptionMismatch(t *testing.T) {
	file := buildFile(t, buildOptions{})
	footerSize := int64(len(file)) - MagicSize - 1 // wrong on purpose
	reader, err := NewReaderWithOptions(bytes.NewReader(file), int64(len(file)), ReaderOptions{FooterSize: &footerSize})
	require.NoError(t, err)
	_, err = reader.ReadFooter()
	require.ErrorContains(t, err, "footer size mismatch")
}

func TestFooterSizeOptionInvalid(t *testing.T) {
	file := buildFile(t, buildOptions{})
	footerSize := int64(1) // too small
	reader, err := NewReaderWithOptions(bytes.NewReader(file), int64(len(file)), ReaderOptions{FooterSize: &footerSize})
	require.NoError(t, err)
	_, err = reader.ReadFooter()
	require.ErrorContains(t, err, "invalid footer size")
}

func TestFooterJSONCorrupt(t *testing.T) {
	file := buildFile(t, buildOptions{corruptFooterPayload: true})
	reader, err := NewReader(bytes.NewReader(file), int64(len(file)))
	require.NoError(t, err)
	_, err = reader.ReadFooter()
	require.ErrorContains(t, err, "decode footer")
}

func TestBlobValidationFailures(t *testing.T) {
	tests := []struct {
		name string
		opts buildOptions
		msg  string
	}{
		{"compression", buildOptions{blobCompressionCodec: "lz4"}, "compression"},
		{"offsetBeforeMagic", buildOptions{overrideBlobOffset: true, blobOffset: 0}, "offset before magic"},
		{"overlapsFooter", buildOptions{blobOffset: MagicSize, blobLength: 10_000}, "extends into footer"},
		{"overlapBlobs", buildOptions{blobOffset: MagicSize, secondBlob: true, secondBlobOffset: MagicSize + 1}, "overlaps or unordered"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			file := buildFile(t, tt.opts)
			reader, err := NewReader(bytes.NewReader(file), int64(len(file)))
			require.NoError(t, err)
			_, err = reader.ReadFooter()
			require.ErrorContains(t, err, tt.msg)
		})
	}
}

func TestReadBlobRequiresFooter(t *testing.T) {
	file := buildFile(t, buildOptions{})
	reader, err := NewReader(bytes.NewReader(file), int64(len(file)))
	require.NoError(t, err)
	_, err = reader.ReadBlob(BlobMetadata{})
	require.ErrorContains(t, err, "footer not read")
}

func TestWriterFinalizedErrors(t *testing.T) {
	b := &bytes.Buffer{}
	w, err := NewWriter(b)
	require.NoError(t, err)
	require.NoError(t, w.AddBlob(BlobMetadata{Type: DeletionVectorV1}, []byte{1}))
	require.NoError(t, w.Finish())

	require.ErrorContains(t, w.AddBlob(BlobMetadata{Type: DeletionVectorV1}, []byte{1}), "writer finalized")
	require.ErrorContains(t, w.SetProperties(nil), "writer finalized")
	require.ErrorContains(t, w.SetCreatedBy("x"), "writer finalized")
	require.ErrorContains(t, w.Finish(), "writer finalized")
}

type buildOptions struct {
	corruptFooterStartMagic bool
	corruptTrailingMagic    bool
	corruptFooterPayload    bool
	footerFlags             uint32
	footerLenOverride       uint32
	blobCompressionCodec    string
	blobOffset              int64
	blobLength              int64
	overrideBlobOffset      bool
	secondBlob              bool
	secondBlobOffset        int64
}

func buildFile(t *testing.T, opts buildOptions) []byte {
	t.Helper()

	blobData := []byte{9, 9, 9}
	blobMeta := BlobMetadata{Type: DeletionVectorV1, SnapshotID: 1, SequenceNumber: 1}
	if opts.overrideBlobOffset {
		blobMeta.Offset = opts.blobOffset
	} else {
		blobMeta.Offset = MagicSize
		if opts.blobOffset != 0 {
			blobMeta.Offset = opts.blobOffset
		}
	}
	if opts.blobLength != 0 {
		blobMeta.Length = opts.blobLength
	} else {
		blobMeta.Length = int64(len(blobData))
	}
	if opts.blobCompressionCodec != "" {
		codec := opts.blobCompressionCodec
		blobMeta.CompressionCodec = &codec
	}

	blobs := []BlobMetadata{blobMeta}
	if opts.secondBlob {
		bm2 := blobMeta
		bm2.Offset = opts.secondBlobOffset
		bm2.Length = int64(len(blobData))
		blobs = append(blobs, bm2)
	}

	footer := Footer{Blobs: blobs, Properties: map[string]string{"k": "v"}}
	footerJSON, err := json.Marshal(footer)
	require.NoError(t, err)
	if opts.corruptFooterPayload {
		footerJSON = []byte("not-json")
	}

	b := &bytes.Buffer{}
	// Leading magic
	b.Write(Magic[:])

	// Blobs
	blobOffset := int64(MagicSize)
	if opts.overrideBlobOffset {
		blobOffset = opts.blobOffset
	} else if opts.blobOffset != 0 {
		blobOffset = opts.blobOffset
	}
	pad := blobOffset - int64(b.Len())
	if pad < 0 {
		pad = 0
	}
	b.Write(bytes.Repeat([]byte{0}, int(pad)))
	firstBlobLen := int(blobs[0].Length)
	b.Write(blobData[:min(firstBlobLen, len(blobData))])

	if opts.secondBlob {
		pad2 := int(opts.secondBlobOffset) - b.Len()
		if pad2 < 0 {
			pad2 = 0
		}
		b.Write(bytes.Repeat([]byte{0}, pad2))
		b.Write(blobData)
	}

	// Footer start magic
	startMagic := Magic
	if opts.corruptFooterStartMagic {
		startMagic = [4]byte{'B', 'A', 'D', '!'}
	}
	b.Write(startMagic[:])

	// Footer payload
	b.Write(footerJSON)

	// Trailer
	trailer := make([]byte, footerTrailerSize)
	footerLen := len(footerJSON)
	if opts.footerLenOverride != 0 {
		footerLen = int(opts.footerLenOverride)
	}
	binary.LittleEndian.PutUint32(trailer[0:4], uint32(footerLen))
	binary.LittleEndian.PutUint32(trailer[4:8], opts.footerFlags)
	trailingMagic := Magic
	if opts.corruptTrailingMagic {
		trailingMagic = [4]byte{'B', 'A', 'D', '!'}
	}
	copy(trailer[8:], trailingMagic[:])
	b.Write(trailer)

	return b.Bytes()
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
