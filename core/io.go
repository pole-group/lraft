// Copyright (c) 2020, pole-group. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package core

import "google.golang.org/protobuf/proto"

type LocalDirReader struct {
	path	string
}

func (ldr *LocalDirReader) readFileWithMeta(fileName string, fileMeta proto.Message, offset, maxCount int64) int64 {

}