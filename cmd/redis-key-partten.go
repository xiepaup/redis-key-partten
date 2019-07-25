package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	log "github.com/sirupsen/logrus"
	"io"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"strconv"
	"sync"
	"xiepaup.com/redis-key-partten/logic"
)

var (
	VERSION     = "xxxx"
	BUILD_DATE  = ""
	COMMIT_SHA1 = ""
)

var (
	file       string
	samples    int64
	confidence int64
	outsample  int
	delimiter  string
	parall     int
	stdIn      bool
	debug      bool
)

func init() {

	flag.BoolVar(&stdIn, "stdin", false, "keys read from pipe ")
	flag.StringVar(&file, "file", "", "keys file name")
	flag.IntVar(&parall, "parall", 5, "sample parall cnt")
	flag.Int64Var(&samples, "samples", 200, "sample keys")
	flag.Int64Var(&confidence, "confidence", 8, "words confidence")
	flag.IntVar(&outsample, "outsample", 4, "output keys sample for every partten")
	flag.StringVar(&delimiter, "delimiter", "auto", "key splited by support : '|',':','#','_'")
	flag.BoolVar(&debug, "debug", false, "enable debug mode")
	perf()
	flag.Usage = usage
}

func usage() {
	fmt.Printf(`------------------------------------------------------------
- redis keylifecycle manage tool               
- auth : xiepaup@163.com
- Version : %s 
- Budild_date : %s 
- commit_sha1 : %s
------------------------------------------------------------
Usage:
	1] ./go-redis-key-partten -file k.redis.keys 
	2] cat k.redis.keys | ./go-redis-key-partten -stdin

Options:
`, VERSION, BUILD_DATE, COMMIT_SHA1)
	flag.PrintDefaults()
}

func perf() {
	go func() {
		mux := http.NewServeMux()
		mux.HandleFunc("/go", func(w http.ResponseWriter, r *http.Request) {
			num := strconv.FormatInt(int64(runtime.NumGoroutine()), 10)
			w.Write([]byte(num))
		})
		log.Println("start goprofile 127.0.0.1:6062/debug/pprof/heap ...")
		http.ListenAndServe("127.0.0.1:6062", nil)
	}()

}

func main() {
	flag.Parse()
	if debug {
		log.SetLevel(log.DebugLevel)
	}
	if file == "" && !stdIn {
		usage()
		os.Exit(1)
	}

	lineCh := make(chan string, 10)

	ctx, cancel := context.WithCancel(context.Background())
	sampleProcs := []*logic.GuessKeyMode{}
	wg := &sync.WaitGroup{}
	wg.Add(parall)
	for i := 0; i < parall; i++ {
		sp := logic.NewGuessKeyMode(samples, confidence, outsample, delimiter)
		go func() {
			defer wg.Done()
			sp.Run(ctx, lineCh)
		}()
		sampleProcs = append(sampleProcs, sp)
	}
	if !stdIn {
		go LoadFileContent(file, lineCh, cancel)
	} else {
		go func() {
			defer close(lineCh)
			defer cancel()
			var c int64
			reader := bufio.NewReader(os.Stdin)
			for {
				c++
				//func (b *Reader) ReadLine() (line []byte, isPrefix bool, err error)
				strBytes, _, err := reader.ReadLine()
				if err != nil {
					if err == io.EOF {
						break
					}
					return
				}
				lineCh <- string(strBytes)
				if c%1800000 == 0 {
					log.Infof("%d doing ...", c)
				}
			}
		}()
	}
	log.Infof("do - key sample begin ...")
	wg.Wait()
	log.Infof("do - key sample done...")

	kpps := []map[string]*logic.KeyPartten{}
	smarts := []map[string]int64{}
	for i := 1; i < len(sampleProcs); i++ {
		kpp := sampleProcs[i].GetSampledKeys()
		kpps = append(kpps, kpp)

		smart := sampleProcs[i].GetSmartWords()
		smarts = append(smarts, smart)
	}

	sampleProcs[0].GetSampledKeys()
	sampleProcs[0].PolymerizationSampledKeys(kpps...)
	sampleProcs[0].ShowSampledKeys()

	sampleProcs[0].GetSmartWords()
	s := sampleProcs[0].PolymerizationSmartWords(smarts...)
	bb, _ := json.Marshal(s)
	log.Infof("this caculated smart words : %s", bb)
}

func LoadFileContent(f string, lineCh chan string, cancle func()) {
	defer close(lineCh)
	defer cancle()
	fh, err := os.Open(f)
	if err != nil {
		return
	}
	fread := bufio.NewReader(fh)

	var c int64
	for {
		c++
		byteLine, _, err := fread.ReadLine()
		if err != nil {
			if err == io.EOF {
				break
			}
			return
		}
		lineCh <- string(byteLine)
		if c%1800000 == 0 {
			log.Infof("%d doing ...", c)
		}
	}
	log.Infof("total polymerization %d keys ,done", c)
}
