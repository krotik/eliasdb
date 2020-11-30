package cluster

import (
	"bytes"
	"fmt"
	"time"
)

/*
msmap is a map of all know  memory-only memberStorages.
*/
var msmap = make(map[*DistributedStorage]*memberStorage)

/*
ClearMSMap clears the current map of known memory-only memberStorages.
*/
func ClearMSMap() {
	msmap = make(map[*DistributedStorage]*memberStorage)
}

/*
DumpMemoryClusterLayout returns the current storage layout in a memory-only cluster
for a given storage manager (e.g. mainPerson.nodes for Person nodes of partition main).
*/
func DumpMemoryClusterLayout(smname string) string {
	buf := new(bytes.Buffer)

	for _, ms := range msmap {
		buf.WriteString(fmt.Sprintf("MemoryStorage: %s\n", ms.gs.Name()))
		buf.WriteString(ms.dump(smname))
	}

	return buf.String()
}

/*
WaitForTransfer waits for the datatransfer to happen.
*/
func WaitForTransfer() {
	for _, ms := range msmap {
		ms.transferWorker()
		for ms.transferRunning {
			time.Sleep(time.Millisecond)
		}
	}
}
