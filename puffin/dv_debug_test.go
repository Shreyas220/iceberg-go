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

package puffin_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"testing"

	"github.com/apache/iceberg-go/puffin"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
)

// TestPrintDVData prints the deletion vector data in detail.
func TestPrintDVData(t *testing.T) {
	data, err := os.ReadFile("testdata/sparse-dv.puffin")
	require.NoError(t, err)

	reader, err := puffin.NewPuffinReader(bytes.NewReader(data), int64(len(data)))
	require.NoError(t, err)

	footer, err := reader.ReadFooter()
	require.NoError(t, err)

	fmt.Println("╔══════════════════════════════════════════════════════════════════╗")
	fmt.Println("║              PUFFIN FILE WITH DELETION VECTOR                    ║")
	fmt.Println("╚══════════════════════════════════════════════════════════════════╝")
	fmt.Printf("\nFile size: %d bytes\n", len(data))
	fmt.Printf("Properties: %v\n", footer.Properties)
	fmt.Printf("Number of blobs: %d\n\n", len(footer.Blobs))

	for i, blob := range footer.Blobs {
		fmt.Printf("┌─────────────────────────────────────────────────────────────────┐\n")
		fmt.Printf("│ BLOB %d: %s\n", i, blob.Type)
		fmt.Printf("├─────────────────────────────────────────────────────────────────┤\n")
		fmt.Printf("│ Offset:          %d (0x%x)\n", blob.Offset, blob.Offset)
		fmt.Printf("│ Length:          %d bytes\n", blob.Length)
		fmt.Printf("│ SnapshotID:      %d\n", blob.SnapshotID)
		fmt.Printf("│ SequenceNumber:  %d\n", blob.SequenceNumber)
		fmt.Printf("│ Fields:          %v\n", blob.Fields)
		fmt.Printf("│ Properties:      %v\n", blob.Properties)
		fmt.Printf("└─────────────────────────────────────────────────────────────────┘\n\n")

		// Read the blob data
		dvData, err := reader.ReadBlob(blob)
		require.NoError(t, err)

		// Print hex dump of the DV
		fmt.Println("DV Raw Data (hex dump):")
		printHexDump(dvData)

		// Parse and explain the DV structure
		fmt.Println("\nDV Structure Breakdown:")
		parseDVStructure(t, dvData)

		// Parse and show deleted positions
		fmt.Println("\nDeleted Positions:")
		dvIndex, err := table.DeserializeDVPayload(dvData, -1) // -1 = don't validate cardinality
		require.NoError(t, err)
		require.NoError(t, dvIndex.Load(context.Background()))

		// Find and print all deleted positions (scan first 2000)
		var deleted []int64
		for pos := int64(0); pos < 2000; pos++ {
			if dvIndex.Contains(pos) {
				deleted = append(deleted, pos)
			}
		}
		fmt.Printf("  Found %d deleted positions: %v\n", len(deleted), deleted)
	}
}

// TestReadByOffsetRange simulates how manifest files reference DV blobs.
// Manifest entries have content_offset and content_size_in_bytes fields.
func TestReadByOffsetRange(t *testing.T) {
	data, err := os.ReadFile("testdata/multiple-dvs.puffin")
	require.NoError(t, err)

	reader, err := puffin.NewPuffinReader(bytes.NewReader(data), int64(len(data)))
	require.NoError(t, err)

	footer, err := reader.ReadFooter()
	require.NoError(t, err)

	fmt.Println("╔══════════════════════════════════════════════════════════════════╗")
	fmt.Println("║     READING DVs BY OFFSET/LENGTH (like manifest entries)         ║")
	fmt.Println("╚══════════════════════════════════════════════════════════════════╝")
	fmt.Println()
	fmt.Println("In a manifest file, each delete file entry has:")
	fmt.Println("  - file_path:            path to the .puffin file")
	fmt.Println("  - content_offset:       offset of the DV blob within the puffin file")
	fmt.Println("  - content_size_in_bytes: length of the DV blob")
	fmt.Println("  - record_count:         number of deleted positions (cardinality)")
	fmt.Println()

	for i, blob := range footer.Blobs {
		// Simulate manifest entry fields
		filePath := "testdata/multiple-dvs.puffin"
		contentOffset := blob.Offset
		contentSize := blob.Length
		referencedDataFile := blob.Properties["referenced-data-file"]

		fmt.Printf("┌─────────────────────────────────────────────────────────────────┐\n")
		fmt.Printf("│ MANIFEST ENTRY %d (simulated)                                   │\n", i)
		fmt.Printf("├─────────────────────────────────────────────────────────────────┤\n")
		fmt.Printf("│ file_path:              %s\n", filePath)
		fmt.Printf("│ content_offset:         %d\n", contentOffset)
		fmt.Printf("│ content_size_in_bytes:  %d\n", contentSize)
		fmt.Printf("│ referenced_data_file:   %s\n", referencedDataFile)
		fmt.Printf("└─────────────────────────────────────────────────────────────────┘\n")

		// Read using offset/range (this is what LoadDVIndex does when offset is known)
		dvData, err := reader.ReadRange(contentOffset, contentSize)
		require.NoError(t, err)

		fmt.Printf("\nRead %d bytes from offset %d:\n", len(dvData), contentOffset)
		printHexDump(dvData)

		// Parse the DV
		dvIndex, err := table.DeserializeDVPayload(dvData, -1)
		require.NoError(t, err)
		require.NoError(t, dvIndex.Load(context.Background()))

		// Find deleted positions
		var deleted []int64
		for pos := int64(0); pos < 100; pos++ {
			if dvIndex.Contains(pos) {
				deleted = append(deleted, pos)
			}
		}
		fmt.Printf("\nDeleted positions: %v\n\n", deleted)
	}
}

// TestCompareFooterVsDirectRead shows that reading via footer == reading via offset
func TestCompareFooterVsDirectRead(t *testing.T) {
	data, err := os.ReadFile("testdata/sparse-dv.puffin")
	require.NoError(t, err)

	reader, err := puffin.NewPuffinReader(bytes.NewReader(data), int64(len(data)))
	require.NoError(t, err)

	footer, err := reader.ReadFooter()
	require.NoError(t, err)

	blob := footer.Blobs[0]

	fmt.Println("╔══════════════════════════════════════════════════════════════════╗")
	fmt.Println("║     COMPARING: ReadBlob() vs ReadRange()                         ║")
	fmt.Println("╚══════════════════════════════════════════════════════════════════╝")
	fmt.Println()

	// Method 1: Read via footer blob metadata
	dvDataViaBlob, err := reader.ReadBlob(blob)
	require.NoError(t, err)

	// Method 2: Read via offset/length (like manifest would)
	dvDataViaRange, err := reader.ReadRange(blob.Offset, blob.Length)
	require.NoError(t, err)

	fmt.Printf("ReadBlob():  %d bytes from blob metadata\n", len(dvDataViaBlob))
	fmt.Printf("ReadRange(): %d bytes from offset=%d, length=%d\n",
		len(dvDataViaRange), blob.Offset, blob.Length)
	fmt.Printf("\nBytes match: %v\n", bytes.Equal(dvDataViaBlob, dvDataViaRange))

	// Both should parse identically
	dv1, err := table.DeserializeDVPayload(dvDataViaBlob, -1)
	require.NoError(t, err)
	require.NoError(t, dv1.Load(context.Background()))

	dv2, err := table.DeserializeDVPayload(dvDataViaRange, -1)
	require.NoError(t, err)
	require.NoError(t, dv2.Load(context.Background()))

	// Check same positions
	positions := []int64{0, 5, 10, 100, 1000}
	fmt.Println("\nPosition check:")
	for _, pos := range positions {
		fmt.Printf("  Position %4d: ReadBlob=%v, ReadRange=%v\n",
			pos, dv1.Contains(pos), dv2.Contains(pos))
	}
}

func printHexDump(data []byte) {
	for i := 0; i < len(data); i += 16 {
		// Print offset
		fmt.Printf("  %04x: ", i)

		// Print hex bytes
		for j := 0; j < 16; j++ {
			if i+j < len(data) {
				fmt.Printf("%02x ", data[i+j])
			} else {
				fmt.Print("   ")
			}
			if j == 7 {
				fmt.Print(" ")
			}
		}

		// Print ASCII
		fmt.Print(" |")
		for j := 0; j < 16 && i+j < len(data); j++ {
			b := data[i+j]
			if b >= 32 && b < 127 {
				fmt.Printf("%c", b)
			} else {
				fmt.Print(".")
			}
		}
		fmt.Println("|")
	}
}

func parseDVStructure(t *testing.T, data []byte) {
	if len(data) < 12 {
		fmt.Println("  Data too short!")
		return
	}

	// Length (4 bytes, Big Endian)
	length := binary.BigEndian.Uint32(data[0:4])
	fmt.Printf("  [0-3]   Length:       %d (0x%08x) - size of magic+bitmap\n", length, length)

	// Magic (4 bytes, Little Endian)
	magic := binary.LittleEndian.Uint32(data[4:8])
	magicValid := "✓"
	if magic != 0x6439D3D1 {
		magicValid = "✗"
	}
	fmt.Printf("  [4-7]   Magic:        0x%08x %s (expected 0x6439D3D1)\n", magic, magicValid)

	// Bitmap data starts at offset 8
	bitmapData := data[8 : len(data)-4]
	fmt.Printf("  [8-%d]  Bitmap:       %d bytes\n", len(data)-5, len(bitmapData))

	// Parse bitmap count if present
	if len(bitmapData) >= 8 {
		bitmapCount := binary.LittleEndian.Uint64(bitmapData[0:8])
		fmt.Printf("          └─ Bitmap count: %d (number of 32-bit key bitmaps)\n", bitmapCount)

		if bitmapCount > 0 && len(bitmapData) >= 12 {
			key := binary.LittleEndian.Uint32(bitmapData[8:12])
			fmt.Printf("          └─ First key:    %d (high 32 bits of positions)\n", key)
		}
	}

	// CRC (4 bytes, Big Endian)
	crc := binary.BigEndian.Uint32(data[len(data)-4:])
	fmt.Printf("  [%d-%d] CRC32:        0x%08x\n", len(data)-4, len(data)-1, crc)
}
