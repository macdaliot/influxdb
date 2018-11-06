package main

import (
	"fmt"
	"math"
	"os"
	"path/filepath"

	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
)

var target = []byte("apache_access_log,host=stream01.va.audionow.com,path=/var/log/httpd/telegraf_access_log,resp_code=200,verb=GET,vhost=player-prod.audionowdigital.com#!~#resp_bytes")

func main() {
	var path = "/Users/edd/.influxdb"

	dataPath := filepath.Join(path, "data")

	// No need to do this in a loop
	ext := fmt.Sprintf(".%s", tsm1.TSMFileExtension)

	// Get all TSM files by walking through the data dir
	files := []string{}
	err := filepath.Walk(dataPath, func(path string, f os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if filepath.Ext(path) == ext {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		panic(err)
	}

	for _, f := range files {
		file, err := os.OpenFile(f, os.O_RDONLY, 0600)
		if err != nil {
			panic(err)
		}
		fmt.Println("Opened file:", f)

		r, err := tsm1.NewTSMReader(file)
		if err != nil {
			panic(err)
		}

		FindTimestamp(r, 1541675791068000000)

		fmin, fmax := r.TimeRange()
		fkcount := r.KeyCount()
		fmt.Printf("File has %d keys. Min time: %v, Max time: %v\n", fkcount, tme(fmin), tme(fmax))

		for i := 0; i < fkcount; i++ {
			key, _ := r.KeyAt(i)
			// if !bytes.Equal(key, target) {
			// 	continue
			// }

			// Found the target key.
			entries := r.Entries(key)
			fmt.Printf("Target key has %d entries (blocks) in index\n", len(entries))
			indexMin, indexMax := int64(math.MaxInt64), int64(math.MinInt64)
			var lastMin, lastMax int64
			for i, e := range entries {
				if e.MinTime < indexMin {
					indexMin = e.MinTime
				}
				if e.MaxTime > indexMax {
					indexMax = e.MaxTime
				}

				var msg string
				if i > 0 {
					if e.MinTime < lastMin {
						msg = fmt.Sprintf("ERROR - Block min time %v is < previous block MIN time %v\n", tme(e.MinTime), tme(lastMin))
					}
					if e.MinTime < lastMax {
						msg = fmt.Sprintf("ERROR - Block min time %v is < previous block MAX time %v\n", tme(e.MinTime), tme(lastMax))
					}
					if e.MaxTime < lastMax {
						msg = fmt.Sprintf("ERROR - Block max time %v is < previous block MAX time %v\n", tme(e.MaxTime), tme(lastMax))
					}
				}
				lastMin, lastMax = e.MinTime, e.MaxTime
				if msg != "" {
					fmt.Printf("[Block %d] MIN: %v, MAX %v. Block Size: %d...%s", e.Offset, tme(e.MinTime), tme(e.MaxTime), e.Size, msg)
				}
			}
			fmt.Printf("Scanned all blocks for target key. Index reports Min: %v, Max: %v\n", tme(indexMin), tme(indexMax))

			// blocksMin, blocksMax := Check(r, entries)
			blocksMin, blocksMax := CheckBatch(r, entries)

			fmt.Printf("Check complete. All blocks scanned Actual MIN: %v, actual MAX: %v\n", tme(blocksMin), tme(blocksMax))
			fmt.Printf("Scanned all blocks for target key. Index reports Min: %v, Max: %v\n", tme(indexMin), tme(indexMax))
		}

		fmt.Println()
	}
}

func Check(r *tsm1.TSMReader, entries []tsm1.IndexEntry) (int64, int64) {
	var blocksMin, blocksMax = int64(math.MaxInt64), int64(math.MinInt64)
	fmt.Println("Checking block-level details...")
	for _, e := range entries {
		var v []tsm1.IntegerValue
		v, err := r.ReadIntegerBlockAt(&e, &v)
		if err != nil {
			panic(err)
		}

		if len(v) < 1 {
			panic("Block does not have any timestamps!")
		}

		if e.MinTime != v[0].UnixNano() {
			fmt.Printf("***Entry min time %d != %d (first timestamp on block)\n", e.MinTime, v[0].UnixNano())
		}
		if e.MaxTime != v[len(v)-1].UnixNano() {
			fmt.Printf("***Entry max time %d != %d (last timestamp on block)\n", e.MaxTime, v[len(v)-1].UnixNano())
		}

		min, max := v[0].UnixNano(), v[len(v)-1].UnixNano()
		if max < min {
			fmt.Printf("[BLOCK %d] ERROR - Block max %v < block min %v\n", e.Offset, tme(max), tme(min))
		}

		last := v[0].UnixNano()
		for _, t := range v {
			if t.UnixNano() < last {
				fmt.Printf("[BLOCK %d] ERROR - Timestamp %v < previous %v\n", e.Offset, tme(t.UnixNano()), tme(last))
			}
			last = t.UnixNano()
		}

		if min < blocksMin {
			blocksMin = min
		}
		if max > blocksMax {
			blocksMax = max
		}
		// fmt.Printf("[BLOCK %d / %d values] MIN: %v, MAX: %v\n", e.Offset, len(v), tme(min), tme(max))
	}
	return blocksMin, blocksMax
}

func CheckBatch(r *tsm1.TSMReader, entries []tsm1.IndexEntry) (int64, int64) {
	var blocksMin, blocksMax = int64(math.MaxInt64), int64(math.MinInt64)
	fmt.Println("Checking block-level details...")
	for _, e := range entries {
		var v = &tsdb.IntegerArray{}
		err := r.ReadIntegerArrayBlockAt(&e, v)
		if err != nil {
			continue
		}

		if e.Offset == 22411699 || e.Offset == 26294803 || e.Offset == 131340023 || e.Offset == 152003909 {
			_, buf, err := r.ReadBytes(&e, nil)
			if err != nil {
				panic(err)
			}
			fmt.Printf("BLOCK %d, TYPE: %d\n", e.Offset, buf[0])
		}

		if len(v.Timestamps) < 1 {
			panic("Block does not have any timestamps!")
		}

		if e.MinTime != v.Timestamps[0] {
			fmt.Printf("Entry min time %d != %d (first timestamp on block)\n", e.MinTime, v.Timestamps[0])
		}
		if e.MaxTime != v.Timestamps[len(v.Timestamps)-1] {
			fmt.Printf("Entry max time %d != %d (last timestamp on block)\n", e.MaxTime, v.Timestamps[len(v.Timestamps)-1])
		}

		min, max := v.Timestamps[0], v.Timestamps[len(v.Timestamps)-1]
		if max < min {
			fmt.Printf("[BLOCK %d] ERROR - Block max %v < block min %v\n", e.Offset, tme(max), tme(min))
		}

		last := v.Timestamps[0]
		for i, t := range v.Timestamps {
			if e.Offset == 22411699 || e.Offset == 26294803 || e.Offset == 131340023 || e.Offset == 152003909 {
				fmt.Printf("[%d] Timestamp: %d, Value: %d\n", i, t, v.Values[i])
			}
			if t < last {
				fmt.Printf("[BLOCK %d] ERROR - Timestamp %v < previous %v\n", e.Offset, tme(t), tme(last))
			}
			last = t
		}

		if min < blocksMin {
			blocksMin = min
		}
		if max > blocksMax {
			blocksMax = max
		}
		fmt.Printf("[BLOCK %d / %d values] MIN: %v, MAX: %v\n", e.Offset, len(v.Timestamps), tme(min), tme(max))
	}
	return blocksMin, blocksMax
}

func FindTimestamp(r *tsm1.TSMReader, timestamp int64) {
	fmin, fmax := r.TimeRange()
	fkcount := r.KeyCount()
	fmt.Printf("File has %d keys. Min time: %v, Max time: %v\n", fkcount, tme(fmin), tme(fmax))

	for i := 0; i < fkcount; i++ {
		key, typ := r.KeyAt(i)

		if typ != 1 {
			continue
		}
		// Found the target key.
		entries := r.Entries(key)
		for _, e := range entries {
			var v = &tsdb.IntegerArray{}
			err := r.ReadIntegerArrayBlockAt(&e, v)
			if err != nil {
				panic(err)
			}

			for _, t := range v.Timestamps {
				if t == timestamp {
					fmt.Printf("[BLOCK %d for Key %q] has timestamp %d. Block MIN: %d, MAX: %d\n", e.Offset, key, t, v.Timestamps[0], v.Timestamps[len(v.Timestamps)-1])
				}
			}
		}
	}
}

func tme(v int64) interface{} {
	return v
	// return time.Unix(0, v)
}
