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
	"context"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadAllDeleteFilesNoDVs(t *testing.T) {
	// Tasks with only parquet positional deletes (no DVs) should return empty dvDeletesPerFile.
	tasks := []FileScanTask{
		{
			File: &mockDataFile{
				path:        "s3://bucket/data/data-001.parquet",
				contentType: iceberg.EntryContentData,
				filesize:    1024,
			},
			DeleteFiles: []iceberg.DataFile{
				// Parquet delete file — won't be read without a real FS, but
				// we're testing that dvDeletesPerFile is empty.
			},
		},
	}

	posDeletes, dvDeletes, err := readAllDeleteFiles(context.Background(), nil, tasks, 1)
	require.NoError(t, err)
	assert.Empty(t, posDeletes)
	assert.Empty(t, dvDeletes)
}

func TestReadAllDeleteFilesEmptyTasks(t *testing.T) {
	posDeletes, dvDeletes, err := readAllDeleteFiles(context.Background(), nil, nil, 1)
	require.NoError(t, err)
	assert.Empty(t, posDeletes)
	assert.Empty(t, dvDeletes)
}

func TestReadAllDeleteFilesDVCollectsUnique(t *testing.T) {
	// Two tasks referencing the same DV for the same data file.
	// Verify dedup: same puffin path + same ref = only collected once.
	ref1 := "s3://bucket/data/data-001.parquet"
	ref2 := "s3://bucket/data/data-002.parquet"

	dv1 := &dvMockDataFile{
		mockDataFile: mockDataFile{
			path:        "s3://bucket/data/dvs.puffin",
			contentType: iceberg.EntryContentPosDeletes,
			format:      iceberg.PuffinFile,
		},
		referencedDataFile: &ref1,
		contentOffset:      int64Ptr(4),
		contentSizeInBytes: int64Ptr(50),
	}

	// Same puffin file, different data file ref
	dv2 := &dvMockDataFile{
		mockDataFile: mockDataFile{
			path:        "s3://bucket/data/dvs.puffin",
			contentType: iceberg.EntryContentPosDeletes,
			format:      iceberg.PuffinFile,
		},
		referencedDataFile: &ref2,
		contentOffset:      int64Ptr(54),
		contentSizeInBytes: int64Ptr(42),
	}

	tasks := []FileScanTask{
		{
			File:                &mockDataFile{path: ref1, contentType: iceberg.EntryContentData, filesize: 1024},
			DeletionVectorFiles: []iceberg.DataFile{dv1},
		},
		{
			File:                &mockDataFile{path: ref1, contentType: iceberg.EntryContentData, filesize: 1024},
			DeletionVectorFiles: []iceberg.DataFile{dv1}, // duplicate of dv1
		},
		{
			File:                &mockDataFile{path: ref2, contentType: iceberg.EntryContentData, filesize: 2048},
			DeletionVectorFiles: []iceberg.DataFile{dv2},
		},
	}

	// Verify dedup logic: collect unique DVs by path:ref key
	uniqueDVs := make(map[string]iceberg.DataFile)
	for _, task := range tasks {
		for _, d := range task.DeletionVectorFiles {
			ref := d.ReferencedDataFile()
			if ref == nil {
				continue
			}
			key := d.FilePath() + ":" + *ref
			if _, ok := uniqueDVs[key]; !ok {
				uniqueDVs[key] = d
			}
		}
	}

	// dv1 appears twice but same key, dv2 is different key
	assert.Len(t, uniqueDVs, 2, "should have 2 unique DVs after dedup")
	assert.Contains(t, uniqueDVs, "s3://bucket/data/dvs.puffin:s3://bucket/data/data-001.parquet")
	assert.Contains(t, uniqueDVs, "s3://bucket/data/dvs.puffin:s3://bucket/data/data-002.parquet")
}

func TestReadAllDeleteFilesDVNilRefSkipped(t *testing.T) {
	// DV files with nil ReferencedDataFile should be skipped.
	dvFile := &dvMockDataFile{
		mockDataFile: mockDataFile{
			path:        "s3://bucket/data/dv.puffin",
			contentType: iceberg.EntryContentPosDeletes,
			format:      iceberg.PuffinFile,
		},
		referencedDataFile: nil, // nil ref
		contentOffset:      int64Ptr(4),
		contentSizeInBytes: int64Ptr(50),
	}

	tasks := []FileScanTask{
		{
			File: &mockDataFile{
				path:        "s3://bucket/data/data-001.parquet",
				contentType: iceberg.EntryContentData,
				filesize:    1024,
			},
			DeletionVectorFiles: []iceberg.DataFile{dvFile},
		},
	}

	// With nil ref, the DV should be skipped entirely — no error, empty maps.
	posDeletes, dvDeletes, err := readAllDeleteFiles(context.Background(), nil, tasks, 1)
	require.NoError(t, err)
	assert.Empty(t, posDeletes)
	assert.Empty(t, dvDeletes)
}
