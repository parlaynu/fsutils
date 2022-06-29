package main

import (
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/parlaynu/fshelpers/iohash"
)

type config struct {
	root     string
	percent  uint64
	nwriters uint64
}

func main() {
	// build configuration from command line
	cfg := parse_cmdline()
	if cfg == nil {
		os.Exit(1)
	}

	// set the random seed
	rand.Seed(time.Now().Unix())

	// create the staging area
	err := os.Mkdir(filepath.Join(cfg.root, "staging"), 0775)
	if err != nil && !os.IsExist(err) {
		fmt.Println("Error: failed to create staging area")
		os.Exit(1)
	}

	// work out how much data to write
	aspace, err := fs_aspace(cfg.root)
	if err != nil {
		fmt.Printf("Error: failed to get freespace on '%s': %s\n", cfg.root, err)
		os.Exit(1)
	}
	total_write := uint64(float64(aspace) * float64(cfg.percent) / 100.0)
	per_writer := total_write / cfg.nwriters

	// run for it
	var wg sync.WaitGroup
	for i := 0; i < int(cfg.nwriters); i++ {
		wg.Add(1)
		go func(idx int, path string, nbytes uint64) {
			defer wg.Done()
			write_files(idx, rand.Int63(), path, nbytes)
		}(i, cfg.root, per_writer)
	}
	wg.Wait()
}

func write_files(idx int, seed int64, root string, nbytes uint64) {
	fmt.Printf("%02d: writing %d bytes to %s\n", idx, nbytes, root)

	rng := rand.New(rand.NewSource(seed))

	count := 0
	for nbytes > 0 {
		size := uint64(1000000 + rng.Int63n(29000000))
		if size > nbytes {
			size = nbytes
		}

		if err := write_file(idx, rng, root, size); err != nil {
			fmt.Printf("%02d: write failed with: %s\n", idx, err)
			break
		}

		nbytes -= size

		count += 1
		if idx == 0 && count%100 == 0 {
			fmt.Printf("%02d: files written: %d\n", idx, count)
		}
	}
}

func write_file(idx int, rng *rand.Rand, root string, size uint64) error {

	tempfile := filepath.Join(root, "staging", fmt.Sprintf("%04d", idx))

	file, err := os.Create(tempfile)
	if err != nil {
		return err
	}
	hasher := iohash.NewSha256HashWriter(file)
	_, err = io.CopyN(hasher, rng, int64(size))
	file.Close()

	hash := hasher.Hash()

	dirname := filepath.Join(root, hash[:4])
	if err := os.Mkdir(dirname, 0775); err != nil && !os.IsExist(err) {
		os.Remove(tempfile)
		return err
	}

	filename := filepath.Join(dirname, hash)
	if err := os.Rename(tempfile, filename); err != nil {
		os.Remove(tempfile)
		return err
	}

	return err
}

func fs_aspace(path string) (uint64, error) {

	fd, err := syscall.Open(path, syscall.O_RDONLY, 0)
	if err != nil {
		return 0, err
	}
	defer syscall.Close(fd)

	var statfs syscall.Statfs_t
	if err := syscall.Fstatfs(fd, &statfs); err != nil {
		return 0, err
	}

	aspace := uint64(statfs.Bsize) * statfs.Bavail

	return aspace, nil
}

func parse_cmdline() *config {

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [-n writers] <path> <percent_fill>\n", filepath.Base(os.Args[0]))
		flag.PrintDefaults()
	}

	nwriters := flag.Uint64("n", 1, "number of concurrent writers")
	flag.Parse()

	if flag.NArg() != 2 {
		fmt.Printf("insufficient arguments provided\n")
		flag.Usage()
		return nil
	}

	var cfg config
	var err error

	cfg.root = flag.Arg(0)
	if cfg.percent, err = strconv.ParseUint(flag.Arg(1), 10, 32); err != nil {
		fmt.Printf("parameter 'percent_fill' must be an integer\n")
		flag.Usage()
		return nil
	}
	cfg.nwriters = *nwriters

	return &cfg
}
