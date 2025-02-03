package ringbuffer

import (
	"os"
	"testing"
)

func TestSetFsync(t *testing.T) {
	tests := []struct {
		name string
		want int
	}{
		{
			name: "no set",
			want: os.O_RDWR | os.O_CREATE,
		},
		{
			name: "set",
			want: os.O_RDWR | os.O_CREATE | os.O_SYNC,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			beforeFOpenFlag := fOpenFlag
			defer func() {
				fOpenFlag = beforeFOpenFlag
			}()

			if tt.name == "set" {
				SetFSync()
			}
			if fOpenFlag != tt.want {
				t.Errorf("flag got = %d, want = %d", fOpenFlag, tt.want)
			}
		})
	}
}
