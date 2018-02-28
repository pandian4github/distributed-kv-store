package util

import (
	"bytes"
	"encoding/gob"
	"strconv"
	"strings"
	"errors"
	"net"
	"time"
	"log"
)

var retryTimes = []float32{0.1, 0.2, 0.2, 0.25, 0.25, 0.25, 0.25, 0.5,
							0.5, 0.5, 1, 1.5, 2, 2.5, 3, 4, 5}
const LOCALHOST_PREFIX = "localhost:"

const (
	HAPPENED_BEFORE = 0
	HAPPENED_AFTER = 1
	CONCURRENT = 2
)

func TotalOrderOfEvents(ts1 map[int]int, s1 int, ts2 map[int]int, s2 int) int {
	// Check if the events are totally ordered
	ordering := HappenedBefore(ts1, ts2)
	if ordering != CONCURRENT {
		return ordering
	}
	// Deterministic ordering if the events are concurrent
	if s1 > s2 {
		return HAPPENED_BEFORE
	}
	if s1 < s2 {
		return HAPPENED_AFTER
	}
	if ts1[s1] < ts2[s1] {
		return HAPPENED_BEFORE
	}
	return HAPPENED_AFTER
}

func HappenedBefore(ts1, ts2 map[int]int) int {
	vecTs1Lesser, vecTs2Lesser := false, false

	for k, v1 := range ts1 {
		if v2, ok := ts2[k]; ok {
			if v1 < v2 {
				vecTs1Lesser = true
			} else if v2 < v1 {
				vecTs2Lesser = true
			}
		} else {
			if v1 > 0 {
				vecTs2Lesser = true
			}
		}
	}
	for k, v2 := range ts2 {
		if v1, ok := ts1[k]; ok {
			if v2 < v1 {
				vecTs2Lesser = true
			} else if v1 < v2 {
				vecTs1Lesser = true
			}
		} else {
			if v2 > 0 {
				vecTs1Lesser = true
			}
		}
	}

	// Either if both timestamps are lesser at some index or if both timestamps are equal in all indices
	if vecTs1Lesser == vecTs2Lesser {
		return CONCURRENT
	}
	if vecTs1Lesser {
		return HAPPENED_BEFORE
	}
	return HAPPENED_AFTER
}

func CopyVecTs(copyFrom map[int]int, copyTo *map[int]int) {
	for k, v := range copyFrom {
		(*copyTo)[k] = v
	}
}

func EncodeMapIntStringToStringGob(mp map[int]string) (string, error) {
	b := new(bytes.Buffer)
	e := gob.NewEncoder(b)

	err := e.Encode(mp)
	return b.String(), err
}

func DecodeMapIntStringFromStringGob(str string) (map[int]string, error) {
	b := bytes.NewBufferString(str)
	d := gob.NewDecoder(b)

	var mp map[int]string
	err := d.Decode(&mp)

	return mp, err
}

/*
Encodes the map into a string in the format key1@value1,key2@value2,....
*/
func EncodeMapIntStringToStringCustom(mp map[int]string) (string, error) {
	var buffer bytes.Buffer
	for k, v := range mp {
		buffer.WriteString(strconv.Itoa(k) + "@" + v + ",")
	}
	return buffer.String(), nil
}

/*
Decodes the map from given string in the format key1@value1,key2@value2,....
*/
func DecodeMapIntStringFromStringCustom(str string) (map[int]string, error) {
	parts := strings.Split(str, ",")

	mp := map[int]string {}
	for _, part := range parts {
		if len(part) == 0 {
			continue
		}
		subParts := strings.Split(part, "@")
		if len(subParts) != 2 {
			return mp, errors.New("unable to decode map from string " + str)
		}
		key, err := strconv.Atoi(subParts[0])
		if err != nil {
			return mp, err
		}
		mp[key] = subParts[1]
	}
	return mp, nil
}

func Sleep(secs float32) {
	time.Sleep(time.Duration(float32(time.Second.Nanoseconds()) * secs))
}

func DialWithRetry(hostPortPair string) (net.Conn, error) {
	numRetries := len(retryTimes)
	var conn net.Conn
	var err error
	for i := 0; i < numRetries; i++ {
		conn, err = net.Dial("tcp", hostPortPair)
		if err != nil {
			log.Println("Unable to connect due to", err, "Waiting for", retryTimes[i], "seconds before retrying..")
			Sleep(retryTimes[i])
			continue
		}
		return conn, nil
	}
	return conn, err
}

/*
func main() {
	var x = map[int]string {
		123: "abc:234",
		124: "def:567",
	}
	str, e := EncodeMapIntStringToString(x)
	if e != nil {
		panic(e)
	}
	log.Println(str)

	y, e2 := DecodeMapIntStringFromString(str)
	if e2 != nil {
		panic(e2)
	}
	log.Println(y)
}
*/