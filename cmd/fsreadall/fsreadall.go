package main

import (
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/parlaynu/fshelpers/iohash"
)

type config struct {
	root     string
	nreaders uint64
}

func main() {
	// build configuration from command line
	cfg := parse_cmdline()
	if cfg == nil {
		os.Exit(1)
	}

	// create the channel to communicate
	ch0 := make(chan string, 2)
	ch1 := make(chan string, 2*cfg.nreaders)

	// create the readers
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		log_reads(ch0, ch1)
	}()
	for i := 0; i < int(cfg.nreaders); i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			read_files(idx, ch1)
		}(i)
	}

	// walk the filesystem and send files to workers
	err := filepath.Walk(cfg.root, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		if filepath.Base(path) == "staging" {
			return nil
		}
		ch0 <- path
		return nil
	})
	if err != nil {
		fmt.Printf("error walking the path %q: %v\n", cfg.root, err)
	}

	// clean up
	close(ch0)
	wg.Wait()
}

type NullWriter struct{}

func (nw NullWriter) Write(p []byte) (int, error) {
	return len(p), nil
}

func log_reads(chi <-chan string, cho chan<- string) {
	var count int64
	var bytes int64

	start := time.Now()

	for fpath := range chi {
		cho <- fpath

		finfo, err := os.Stat(fpath)
		if err != nil {
			continue
		}
		bytes += finfo.Size()

		count += 1
		if count%100 == 0 {
			fmt.Printf("info: files: %s, bytes: %s\n", humanize.Comma(count), humanize.Bytes(uint64(bytes)))
		}
	}
	close(cho)

	duration := time.Since(start)
	rate := float64(bytes) / duration.Seconds() / 1024.0 / 1024.0

	fmt.Printf("read %s files (%s) in %s minutes at %s MB/s\n",
		humanize.Comma(count),
		humanize.Bytes(uint64(bytes)),
		humanize.FtoaWithDigits(duration.Minutes(), 4),
		humanize.FtoaWithDigits(rate, 4))
}

func read_files(idx int, ch <-chan string) {
	for fpath := range ch {
		if err := read_file(idx, fpath); err != nil {
			fmt.Printf("Error: %02d: failed to read '%s' with '%s'", idx, fpath, err)
			return
		}
	}
}

func read_file(idx int, fpath string) error {
	file, err := os.Open(fpath)
	if err != nil {
		return err
	}
	defer file.Close()

	hasher := iohash.NewSha256HashReader(file)

	var nw NullWriter
	_, err = io.Copy(nw, hasher)
	if err != nil {
		return err
	}

	hash := hasher.Hash()
	name := filepath.Base(fpath)

	if hash != name {
		fmt.Printf("%02d: bad hash for file %s: %s\n", idx, name, hash)
	}

	return nil
}

func parse_cmdline() *config {

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [-n readers] <path>\n", filepath.Base(os.Args[0]))
		flag.PrintDefaults()
	}

	nreaders := flag.Uint64("n", 1, "number of concurrent readers")
	flag.Parse()

	if flag.NArg() != 1 {
		fmt.Printf("insufficient arguments provided\n")
		flag.Usage()
		return nil
	}

	var cfg config
	cfg.root = flag.Arg(0)
	cfg.nreaders = *nreaders

	return &cfg
}
