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
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math"
)

// PuffinReader provides access to a Puffin file's blobs and metadata.
// It uses io.ReaderAt to allow efficient random access to blobs.
type PuffinReader struct {
	r           io.ReaderAt
	size        int64
	footerStart int64
	footerRead  bool
}

// NewPuffinReader creates a new Puffin reader.
func NewPuffinReader(r io.ReaderAt, size int64) (*PuffinReader, error) {
	if r == nil {
		return nil, fmt.Errorf("puffin: reader is nil")
	}
	if size < MagicSize+footerTrailerSize {
		return nil, fmt.Errorf("puffin: file too small (%d bytes)", size)
	}

	// Verify leading magic
	var head [4]byte
	if _, err := r.ReadAt(head[:], 0); err != nil {
		return nil, fmt.Errorf("puffin: read leading magic: %w", err)
	}
	if head != Magic {
		return nil, fmt.Errorf("puffin: invalid leading magic")
	}

	// Verify trailing magic
	var magicBuf [4]byte
	if _, err := r.ReadAt(magicBuf[:], size-MagicSize); err != nil {
		return nil, fmt.Errorf("puffin: read trailing magic: %w", err)
	}
	if magicBuf != Magic {
		return nil, fmt.Errorf("puffin: invalid trailing magic")
	}

	return &PuffinReader{r: r, size: size}, nil
}

// ReadFooter reads and parses the file footer.
func (r *PuffinReader) ReadFooter() (*Footer, error) {
	offset := r.size - footerTrailerSize
	var trailer [8]byte // Length + Flags

	if _, err := r.r.ReadAt(trailer[:], offset); err != nil {
		return nil, fmt.Errorf("puffin: read footer trailer: %w", err)
	}
	trailerFooterLen := int64(binary.LittleEndian.Uint32(trailer[0:4]))
	flags := binary.LittleEndian.Uint32(trailer[4:8])

	footerLen := trailerFooterLen

	if flags&FooterFlagCompressed != 0 {
		return nil, fmt.Errorf("puffin: compressed footer unsupported")
	}
	if flags>>1 != 0 {
		return nil, fmt.Errorf("puffin: unknown footer flags set: 0x%x", flags)
	}

	if footerLen < 0 {
		return nil, fmt.Errorf("puffin: invalid footer length %d", footerLen)
	}
	if footerLen > math.MaxInt32 {
		return nil, fmt.Errorf("puffin: footer length %d exceeds 2GB limit", footerLen)
	}
	if footerLen > math.MaxInt64-int64(MagicSize)-footerTrailerSize {
		return nil, fmt.Errorf("puffin: footer length %d overflows", footerLen)
	}
	footerSize := int64(MagicSize) + footerLen + footerTrailerSize
	if footerSize > r.size {
		return nil, fmt.Errorf("puffin: footer length %d exceeds file size %d", footerLen, r.size)
	}
	if footerSize < MagicSize+footerTrailerSize {
		return nil, fmt.Errorf("puffin: invalid footer size %d", footerSize)
	}

	footerStart := r.size - footerSize
	// Check Footer Start Magic
	var startMagic [4]byte
	if _, err := r.r.ReadAt(startMagic[:], footerStart); err != nil {
		return nil, fmt.Errorf("puffin: read footer start magic: %w", err)
	}
	if startMagic != Magic {
		return nil, fmt.Errorf("puffin: invalid footer start magic")
	}

	payloadStart := footerStart + MagicSize
	payloadEnd := payloadStart + footerLen
	if payloadStart < MagicSize || payloadEnd > offset {
		return nil, fmt.Errorf("puffin: footer payload out of bounds start=%d end=%d", payloadStart, payloadEnd)
	}

	// Read Footer Payload
	payload := make([]byte, footerLen)
	if _, err := r.r.ReadAt(payload, payloadStart); err != nil {
		return nil, fmt.Errorf("puffin: read footer payload: %w", err)
	}

	var f Footer
	if err := json.Unmarshal(payload, &f); err != nil {
		return nil, fmt.Errorf("puffin: decode footer: %w", err)
	}

	if err := validateBlobs(f.Blobs, footerStart); err != nil {
		return nil, fmt.Errorf("puffin: footer blobs invalid: %w", err)
	}

	r.footerStart = footerStart
	r.footerRead = true
	return &f, nil
}

// ReadBlob reads the content of a specific blob.
func (r *PuffinReader) ReadBlob(b BlobMetadata) ([]byte, error) {
	if !r.footerRead {
		return nil, fmt.Errorf("puffin: footer not read")
	}

	if b.Length < 0 {
		return nil, fmt.Errorf("puffin: negative blob length")
	}
	if b.Type == "" {
		return nil, fmt.Errorf("puffin: blob type required")
	}
	if b.CompressionCodec != nil && *b.CompressionCodec != "" {
		return nil, fmt.Errorf("puffin: blob compression not supported")
	}
	end := b.Offset + b.Length
	if end < b.Offset || b.Offset < MagicSize {
		return nil, fmt.Errorf("puffin: blob offset/length invalid offset=%d length=%d", b.Offset, b.Length)
	}

	if r.footerRead {
		startLimit := r.footerStart
		if end > startLimit {
			return nil, fmt.Errorf("puffin: blob out of bounds offset=%d length=%d end=%d footerStart=%d",
				b.Offset, b.Length, end, startLimit)
		}
	} else {
		if end > r.size {
			return nil, fmt.Errorf("puffin: blob extends past end of file offset=%d length=%d fileSize=%d",
				b.Offset, b.Length, r.size)
		}
	}

	data := make([]byte, b.Length)
	if _, err := r.r.ReadAt(data, b.Offset); err != nil {
		return nil, fmt.Errorf("puffin: read blob: %w", err)
	}

	return data, nil
}

// ReadRange reads a raw byte range with Puffin boundary checks.
func (r *PuffinReader) ReadRange(offset, length int64) ([]byte, error) {
	if !r.footerRead {
		return nil, fmt.Errorf("puffin: footer not read")
	}
	if length < 0 {
		return nil, fmt.Errorf("puffin: negative length")
	}
	end := offset + length
	if end < offset || offset < MagicSize {
		return nil, fmt.Errorf("puffin: offset/length invalid offset=%d length=%d", offset, length)
	}
	if end > r.footerStart {
		return nil, fmt.Errorf("puffin: range out of bounds offset=%d length=%d end=%d footerStart=%d",
			offset, length, end, r.footerStart)
	}

	buf := make([]byte, length)
	if _, err := r.r.ReadAt(buf, offset); err != nil {
		return nil, fmt.Errorf("puffin: read range: %w", err)
	}
	return buf, nil
}

// BlobData pairs a blob's metadata with its content.
type BlobData struct {
	Metadata BlobMetadata
	Data     []byte
}

// ReadAllBlobs reads multiple blobs, sorted by offset for sequential I/O efficiency.
// Returns results in the same order as the input blobs.
func (r *PuffinReader) ReadAllBlobs(blobs []BlobMetadata) ([]BlobData, error) {
	if !r.footerRead {
		return nil, fmt.Errorf("puffin: footer not read")
	}
	if len(blobs) == 0 {
		return nil, nil
	}

	// Create index mapping for original order
	type indexedBlob struct {
		index int
		blob  BlobMetadata
	}
	indexed := make([]indexedBlob, len(blobs))
	for i, b := range blobs {
		indexed[i] = indexedBlob{index: i, blob: b}
	}

	// Sort by offset for sequential I/O
	for i := 0; i < len(indexed)-1; i++ {
		for j := i + 1; j < len(indexed); j++ {
			if indexed[j].blob.Offset < indexed[i].blob.Offset {
				indexed[i], indexed[j] = indexed[j], indexed[i]
			}
		}
	}

	// Read in offset order
	results := make([]BlobData, len(blobs))
	for _, ib := range indexed {
		data, err := r.ReadBlob(ib.blob)
		if err != nil {
			return nil, fmt.Errorf("puffin: read blob %d: %w", ib.index, err)
		}
		results[ib.index] = BlobData{Metadata: ib.blob, Data: data}
	}

	return results, nil
}

func validateBlobs(blobs []BlobMetadata, footerStart int64) error {
	for i, b := range blobs {
		if b.Type == "" {
			return fmt.Errorf("blob %d: type required", i)
		}
		if b.Length < 0 {
			return fmt.Errorf("blob %d: length negative %d", i, b.Length)
		}
		if b.CompressionCodec != nil && *b.CompressionCodec != "" {
			return fmt.Errorf("blob %d: compression not supported", i)
		}

		if b.Offset < MagicSize {
			return fmt.Errorf("blob %d: offset before magic offset=%d", i, b.Offset)
		}
		end := b.Offset + b.Length
		if end < b.Offset {
			return fmt.Errorf("blob %d: offset overflow offset=%d length=%d", i, b.Offset, b.Length)
		}
		if end > footerStart {
			return fmt.Errorf("blob %d: extends into footer offset=%d length=%d footerStart=%d", i, b.Offset, b.Length, footerStart)
		}
	}
	return nil
}
