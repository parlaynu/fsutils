package main

import (
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/dustin/go-humanize"
)

type config struct {
	root     string
	duration time.Duration
	nreaders uint64
}

func main() {
	// build configuration from command line
	cfg := parse_cmdline()
	if cfg == nil {
		os.Exit(1)
	}

	// seed the random number generator
	rand.Seed(time.Now().Unix())

	// create the channel to communicate
	ch0 := make(chan string, 2)
	ch1 := make(chan string, 2*cfg.nreaders)

	// create the readers
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		filter_files(ch0, ch1)
	}()
	for i := 0; i < int(cfg.nreaders); i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			read_files(idx, ch1)
		}(i)
	}

	start := time.Now()
	for time.Since(start) < cfg.duration {
		scan(cfg.root, ch0, start, cfg.duration)
	}

	// clean up
	close(ch0)
	wg.Wait()
}

func scan(root string, cho chan<- string, start time.Time, duration time.Duration) {

	file, err := os.Open(root)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer file.Close()

	for true {
		if time.Since(start) > duration {
			break
		}

		entries, err := file.ReadDir(10)
		if err != nil {
			if err != io.EOF {
				fmt.Println(err)
			}
			break
		}

		for _, entry := range entries {
			if entry.Name() == "staging" {
				continue
			}
			fpath := filepath.Join(root, entry.Name())
			if entry.Type().IsRegular() {
				cho <- fpath

			} else if entry.IsDir() {
				scan(fpath, cho, start, duration)

			}
		}
	}
}

func filter_files(chi <-chan string, cho chan<- string) {
	start := time.Now()

	var count int64

	for fpath := range chi {
		// only process some files
		if rand.Intn(4) != 2 {
			continue
		}

		// and replicate them a few times
		repl := 1 + rand.Intn(3)
		for i := 0; i < repl; i++ {
			cho <- fpath
		}

		count += 1
		if count%1000 == 0 {
			fmt.Printf("info: file count: %s\n", humanize.Comma(count))
		}
	}
	close(cho)

	duration := time.Since(start)

	fmt.Printf("read from %s files in %s minutes\n",
		humanize.Comma(count),
		humanize.FtoaWithDigits(duration.Minutes(), 4))
}

func read_files(idx int, ch <-chan string) {
	for fpath := range ch {
		if err := read_file(idx, fpath); err != nil {
			fmt.Printf("Error: %02d: failed to read '%s' with '%s'", idx, fpath, err)
			return
		}
	}
}

type NullWriter struct{}

func (nw NullWriter) Write(p []byte) (int, error) {
	return len(p), nil
}

func read_file(idx int, fpath string) error {
	// calculate how much of the file to read and where to start
	finfo, err := os.Stat(fpath)
	if err != nil {
		return err
	}

	offset := int64(float32(rand.Intn(10)) / 10.0 * float32(finfo.Size()))
	length := int64(float32(1+rand.Intn(5)) / 10.0 * float32(finfo.Size()))

	// read the data
	file, err := os.Open(fpath)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.Seek(offset, os.SEEK_SET)
	if err != nil {
		return err
	}

	var nw NullWriter
	_, err = io.CopyN(nw, file, length)
	if err != nil && err != io.EOF {
		return err
	}

	return nil
}

func parse_cmdline() *config {

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [-n readers] <path> <duration>\n", filepath.Base(os.Args[0]))
		flag.PrintDefaults()
	}

	nreaders := flag.Uint64("n", 1, "number of concurrent readers")
	flag.Parse()

	if flag.NArg() != 2 {
		fmt.Printf("insufficient arguments provided\n")
		flag.Usage()
		return nil
	}

	var cfg config
	cfg.root = flag.Arg(0)

	duration, err := time.ParseDuration(flag.Arg(1))
	if err != nil {
		fmt.Printf("failed to parse duration: %s\n", err)
		flag.Usage()
		return nil
	}
	cfg.duration = duration
	cfg.nreaders = *nreaders

	return &cfg
}
