// Copyright (c) 2020, pole-group. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package utils

import polerpc "github.com/pole-group/pole-rpc"

var RaftDefaultScheduler = polerpc.NewRoutinePool(256, 128)
