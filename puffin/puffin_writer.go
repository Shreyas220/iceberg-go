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

// Writer writes a Puffin file to an output stream.
type Writer struct {
	w         io.Writer
	offset    int64
	blobs     []BlobMetadata
	props     map[string]string
	done      bool
	createdBy string
}

// NewWriter creates a new Puffin writer.
func NewWriter(w io.Writer) (*Writer, error) {
	if w == nil {
		return nil, fmt.Errorf("puffin: writer is nil")
	}
	// Write leading magic immediately
	if _, err := w.Write(Magic[:]); err != nil {
		return nil, fmt.Errorf("puffin: write leading magic: %w", err)
	}
	return &Writer{
		w:      w,
		offset: MagicSize,
		props:  make(map[string]string),
	}, nil
}

// SetProperties sets the file-level properties.
func (w *Writer) SetProperties(props map[string]string) error {
	if w.done {
		return fmt.Errorf("puffin: writer finalized")
	}
	if w.props == nil {
		w.props = make(map[string]string)
	}
	for k, v := range props {
		w.props[k] = v
	}
	return nil
}

// SetCreatedBy records the writer identifier/version in the footer properties.
func (w *Writer) SetCreatedBy(createdBy string) error {
	if w.done {
		return fmt.Errorf("puffin: writer finalized")
	}
	if createdBy == "" {
		return fmt.Errorf("puffin: created-by required")
	}
	w.createdBy = createdBy
	return nil
}

// AddBlob writes a blob to the file and records its metadata.
func (w *Writer) AddBlob(meta BlobMetadata, data []byte) error {
	if w.done {
		return fmt.Errorf("puffin: writer finalized")
	}
	if meta.Type == "" {
		return fmt.Errorf("puffin: blob type required")
	}
	if meta.Length < 0 {
		return fmt.Errorf("puffin: blob length negative: %d", meta.Length)
	}
	if meta.CompressionCodec != nil && *meta.CompressionCodec != "" {
		return fmt.Errorf("puffin: blob compression not supported")
	}
	meta.Offset = w.offset
	meta.Length = int64(len(data))
	meta.CompressionCodec = nil

	if _, err := w.w.Write(data); err != nil {
		return fmt.Errorf("puffin: write blob: %w", err)
	}

	w.offset += meta.Length
	w.blobs = append(w.blobs, meta)
	return nil
}

// Finish writes the footer and closes the file structure (does not close the underlying writer).
func (w *Writer) Finish() error {
	if w.done {
		return fmt.Errorf("puffin: writer finalized")
	}
	footer := Footer{
		Blobs:      w.blobs,
		Properties: w.props,
	}
	if footer.Properties == nil {
		footer.Properties = make(map[string]string)
	}
	if w.createdBy != "" {
		footer.Properties[CreatedBy] = w.createdBy
	}

	payload, err := json.Marshal(footer)
	if err != nil {
		return fmt.Errorf("puffin: marshal footer: %w", err)
	}
	if len(payload) > math.MaxUint32 {
		return fmt.Errorf("puffin: footer too large: %d bytes", len(payload))
	}

	// Write Footer Start Magic
	if _, err := w.w.Write(Magic[:]); err != nil {
		return fmt.Errorf("puffin: write footer start magic: %w", err)
	}

	// Write Footer Payload
	if _, err := w.w.Write(payload); err != nil {
		return fmt.Errorf("puffin: write footer payload: %w", err)
	}

	// Write Trailer: Length (4) + Flags (4) + Magic (4)
	var trailer [12]byte
	binary.LittleEndian.PutUint32(trailer[0:4], uint32(len(payload)))
	binary.LittleEndian.PutUint32(trailer[4:8], 0) // Flags
	copy(trailer[8:], Magic[:])

	if _, err := w.w.Write(trailer[:]); err != nil {
		return fmt.Errorf("puffin: write footer trailer: %w", err)
	}

	w.done = true
	return nil
}
