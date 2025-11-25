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

package table

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"

	"github.com/RoaringBitmap/roaring"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/puffin"
)

const (
	// DVMagicNumber is the magic number for deletion vectors.
	// Spec: D1 D3 39 64 (Big Endian bytes) -> 0x6439D3D1 (Little Endian uint32)
	DVMagicNumber = 0x6439D3D1
)

// PositionDeleteIndex is an interface for checking if a row position is deleted.
type PositionDeleteIndex interface {
	// Contains returns true if the position is marked as deleted.
	Contains(pos int64) bool
	// IsEmpty returns true if the index contains no deletes.
	IsEmpty() bool
	// Release releases any resources held by the index.
	Release()
}

// ParquetPositionDeleteIndex implements PositionDeleteIndex using a hash map.
// Used for position deletes loaded from Parquet delete files.
type ParquetPositionDeleteIndex struct {
	positions map[int64]struct{}
}

func (idx *ParquetPositionDeleteIndex) Contains(pos int64) bool {
	_, ok := idx.positions[pos]
	return ok
}

func (idx *ParquetPositionDeleteIndex) IsEmpty() bool {
	return len(idx.positions) == 0
}

func (idx *ParquetPositionDeleteIndex) Release() {
	// No-op: Go's GC handles map cleanup
}

// RoaringPositionDeleteIndex implements PositionDeleteIndex using Roaring Bitmaps.
// It handles 64-bit positions by splitting them into a high 32-bit key and a low 32-bit bitmap.
type RoaringPositionDeleteIndex struct {
	// highBits maps the high 32 bits of the position to a Roaring Bitmap of the low 32 bits.
	highBits map[uint32]*roaring.Bitmap
}

func NewRoaringPositionDeleteIndex() *RoaringPositionDeleteIndex {
	return &RoaringPositionDeleteIndex{
		highBits: make(map[uint32]*roaring.Bitmap),
	}
}

func (idx *RoaringPositionDeleteIndex) Contains(pos int64) bool {
	if pos < 0 {
		return false
	}
	high := uint32(pos >> 32)
	low := uint32(pos)

	if bm, ok := idx.highBits[high]; ok {
		return bm.Contains(low)
	}
	return false
}

func (idx *RoaringPositionDeleteIndex) Release() {
	// No-op for now, as Roaring Bitmaps are Go objects managed by GC.
}

func (idx *RoaringPositionDeleteIndex) IsEmpty() bool {
	return len(idx.highBits) == 0
}

// DeserializeDVPayload deserializes a Deletion Vector payload from a byte slice.
// The format is:
// - Length (4 bytes, Big Endian)
// - Magic (4 bytes, Little Endian check / Big Endian bytes)
// - Bitmap (Variable, Roaring Portable Format)
// - CRC32 (4 bytes, Big Endian)
func DeserializeDVPayload(data []byte, expectedCardinality int64) (*RoaringPositionDeleteIndex, error) {
	if len(data) < 12 { // 4 len + 4 magic + 4 crc (min)
		return nil, fmt.Errorf("deletion vector payload too short: %d bytes", len(data))
	}

	// 1. Read Length (Big Endian)
	// Spec: "Combined length of the vector and magic bytes"
	length := binary.BigEndian.Uint32(data[0:4])

	// Total file size = 4 (Length Field) + Length Value + 4 (CRC)
	// So Length Value should be len(data) - 8
	if int(length) != len(data)-8 {
		return nil, fmt.Errorf("deletion vector length mismatch: expected %d, got %d", length, len(data)-8)
	}

	// 2. Read Magic (Little Endian check)
	// Bytes 4-8 are Magic.
	magic := binary.LittleEndian.Uint32(data[4:8])
	if magic != DVMagicNumber {
		return nil, fmt.Errorf("invalid deletion vector magic: 0x%x", magic)
	}

	// 3. Verify CRC32 (Big Endian)
	// CRC is calculated over Magic + Bitmap (bytes 4 to end-4)
	expectedCRC := binary.BigEndian.Uint32(data[len(data)-4:])
	calculatedCRC := crc32.ChecksumIEEE(data[4 : len(data)-4])
	if calculatedCRC != expectedCRC {
		return nil, fmt.Errorf("deletion vector CRC mismatch: expected %d, got %d", expectedCRC, calculatedCRC)
	}

	// 4. Deserialize Bitmap (Roaring Portable Format for 64-bit)
	// The bitmap data starts at offset 8 and ends at len(data)-4
	bitmapData := data[8 : len(data)-4]
	buf := bytes.NewReader(bitmapData)

	idx := NewRoaringPositionDeleteIndex()
	var totalCardinality int64 = 0

	// Read number of bitmaps (8 bytes, Little Endian)
	var numBitmaps uint64
	if err := binary.Read(buf, binary.LittleEndian, &numBitmaps); err != nil {
		if err == io.EOF && len(bitmapData) == 0 {
			// Empty bitmap case
			return idx, nil
		}
		return nil, fmt.Errorf("failed to read number of bitmaps: %w", err)
	}

	for i := 0; i < int(numBitmaps); i++ {
		// Read Key (4 bytes, Little Endian)
		var key uint32
		if err := binary.Read(buf, binary.LittleEndian, &key); err != nil {
			return nil, fmt.Errorf("failed to read bitmap key %d: %w", i, err)
		}

		// Read Bitmap
		bm := roaring.New()
		if _, err := bm.ReadFrom(buf); err != nil {
			return nil, fmt.Errorf("failed to read bitmap for key %d: %w", key, err)
		}

		idx.highBits[key] = bm
		totalCardinality += int64(bm.GetCardinality())
	}

	// 5. Validate Cardinality
	if expectedCardinality >= 0 && totalCardinality != expectedCardinality {
		return nil, fmt.Errorf("deletion vector cardinality mismatch: expected %d, got %d", expectedCardinality, totalCardinality)
	}

	return idx, nil
}

// LoadDVIndex loads a Deletion Vector index from a delete file.
// It optimizes for Puffin files by reading the blob directly if offset/size are known.
func LoadDVIndex(ctx context.Context, fs iceio.IO, dvFile iceberg.DataFile) (PositionDeleteIndex, error) {
	if dvFile.FileFormat() != iceberg.PuffinFile {
		return nil, fmt.Errorf("unsupported file format for deletion vector: %s", dvFile.FileFormat())
	}

	f, err := fs.Open(dvFile.FilePath())
	if err != nil {
		return nil, fmt.Errorf("failed to open DV file %s: %w", dvFile.FilePath(), err)
	}
	defer f.Close()

	var blobData []byte

	// Optimization: If we have the offset and size, read directly (skip footer parsing)
	if dvFile.ContentOffset() != nil && dvFile.ContentSizeInBytes() != nil {
		offset := *dvFile.ContentOffset()
		size := *dvFile.ContentSizeInBytes()

		blobData = make([]byte, size)
		if _, err := f.ReadAt(blobData, offset); err != nil {
			return nil, fmt.Errorf("failed to read DV blob at offset %d: %w", offset, err)
		}
	} else {
		// Fallback: Use Puffin reader to find the blob
		// We need the file size for Puffin reader
		stat, err := f.Stat()
		if err != nil {
			return nil, fmt.Errorf("failed to stat DV file: %w", err)
		}

		pr, err := puffin.NewReader(f, stat.Size())
		if err != nil {
			return nil, fmt.Errorf("failed to create puffin reader: %w", err)
		}

		footer, err := pr.ReadFooter()
		if err != nil {
			return nil, fmt.Errorf("failed to read puffin footer: %w", err)
		}

		// Find the DV blob
		var dvBlob *puffin.BlobMetadata
		for _, blob := range footer.Blobs {
			if blob.Type == "deletion-vector-v1" {
				// We take the first DV blob we find.
				// In theory, we could check "referenced-data-file" property,
				// but the Manifest entry is the authoritative pointer.
				b := blob
				dvBlob = &b
				break
			}
		}

		if dvBlob == nil {
			return nil, fmt.Errorf("no deletion-vector-v1 blob found in puffin file")
		}

		blobData, err = pr.ReadBlob(*dvBlob)
		if err != nil {
			return nil, fmt.Errorf("failed to read DV blob: %w", err)
		}
	}

	// Deserialize the payload
	return DeserializeDVPayload(blobData, dvFile.Count())
}
