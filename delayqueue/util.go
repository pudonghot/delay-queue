package delayqueue

import (
	"bytes"
	log "github.com/sirupsen/logrus"
	"strconv"
	"time"
)

func wrap(body []byte, at time.Time) []byte {
	var buf bytes.Buffer
	buf.WriteString(strconv.FormatInt(at.UnixNano(), 10))
	buf.WriteByte(timeSep)
	buf.Write(body)
	return buf.Bytes()
}

func getMaxTimeLen() int {
	return len(strconv.FormatInt(time.Now().UnixNano(), 10)) + 2
}

var maxCheckBytes = getMaxTimeLen()

func unwrap(body []byte) ([]byte, bool) {
	pos := -1
	for i := 0; i < maxCheckBytes && i < len(body); i++ {
		if body[i] == timeSep {
			pos = i
			break
		}
	}
	if pos < 0 {
		return nil, false
	}

	timeVal, err := strconv.ParseInt(string(body[:pos]), 10, 64)
	if err != nil {
		log.Error(err)
		return nil, false
	}

	t := time.Unix(0, timeVal)
	if t.Add(tolerance).Before(time.Now()) {
		return nil, false
	}

	return body[pos+1:], true
}
