package hdfs

import (
	"errors"
	"os"

	hdfs "github.com/colinmarc/hdfs/protocol/hadoop_hdfs"
	"github.com/colinmarc/hdfs/rpc"
	"github.com/golang/protobuf/proto"
)

// Truncate truncates the named file, http://man7.org/linux/man-pages/man2/ftruncate.2.html
func (c *Client) Truncate(name string, size uint64) error {
	_, err := c.getFileInfo(name)
	if err != nil {
		return &os.PathError{"truncate", name, err}
	}

	req := &hdfs.TruncateRequestProto {
		Src:        proto.String(name),
		NewLength:  proto.Uint64(size),
		ClientName: proto.String(c.namenode.ClientName()),
	}
	resp := &hdfs.TruncateResponseProto{}

	err = c.namenode.Execute("truncate", req, resp)
	if err != nil {
		if nnErr, ok := err.(*rpc.NamenodeError); ok {
			err = interpretException(nnErr.Exception, err)
		}

		return &os.PathError{"truncate", name, err}
	} else if resp.Result == nil {
		return &os.PathError{
			"truncate",
			name,
			errors.New("Unexpected empty reponse to 'truncate' rpc call"),
		}
	}

	return nil
}
