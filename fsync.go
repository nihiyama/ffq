package ffq

import "os"

var (
	fOpenFlag   int = os.O_RDWR | os.O_CREATE
	fCreateFlag     = os.O_RDWR | os.O_CREATE | os.O_TRUNC
)

func SetFSync() {
	fOpenFlag = os.O_RDWR | os.O_CREATE | os.O_SYNC
	fCreateFlag = os.O_RDWR | os.O_CREATE | os.O_TRUNC | os.O_SYNC
}
