package hdfs

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTruncate(t *testing.T) {
	client := getClient(t)

	touch(tm, "/_test/totruncate")
	resp, _ := client.Stat("/_test/totruncate")

	err := client.Truncate("/_test/totruncate", resp.Size() + 4)
	require.NoError(t, err)

	err := client.Truncate("/_test/totruncate", resp.Size())
	require.NoError(t, err)
}

func TestTruncateNotExistent(t *testing.T) {
	client := getClient(t)

	err := client.Truncate("/_test/nonexistent", 4)
	assertPathError(t, err, "truncate", "/_test/nonexistent", os.ErrNotExist)
}
