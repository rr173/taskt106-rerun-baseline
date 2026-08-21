package fencing

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
)

func makeToken(path string, sequence int64) string {
	digest := sha256.Sum256([]byte(path + ":" + strconv.FormatInt(sequence, 10)))
	return fmt.Sprintf("f1.%d.%s", sequence, hex.EncodeToString(digest[:12]))
}
