package cache

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"

	"github.com/meltwater/drone-cache/archive"
	"github.com/meltwater/drone-cache/internal"
	"github.com/meltwater/drone-cache/key"
	"github.com/meltwater/drone-cache/storage"
	"github.com/meltwater/drone-cache/storage/common"
)

const (
	PLUGIN_CACHE_INTEL_FILE_NAME = "plugin-cache-intel.json"
)

type restorer struct {
	logger log.Logger

	a  archive.Archive
	s  storage.Storage
	g  key.Generator
	fg key.Generator

	namespace               string
	failIfKeyNotPresent     bool
	enableCacheKeySeparator bool
	backend                 string
	accountID               string
}

// NewRestorer creates a new cache.Restorer.
func NewRestorer(logger log.Logger, s storage.Storage, a archive.Archive, g key.Generator, fg key.Generator, namespace string, failIfKeyNotPresent bool, enableCacheKeySeparator bool, backend, accountID string) Restorer { // nolint:lll
	return restorer{logger, a, s, g, fg, namespace, failIfKeyNotPresent, enableCacheKeySeparator, backend, accountID}
}

// Restore restores files from the cache provided with given paths.
func (r restorer) Restore(dsts []string) error {
	level.Info(r.logger).Log("msg", "restoring cache")

	now := time.Now()

	key, err := r.generateKey()
	if err != nil {
		return fmt.Errorf("generate key, %w", err)
	}

	var (
		wg        sync.WaitGroup
		errs      = &internal.MultiError{}
		namespace = filepath.ToSlash(filepath.Clean(r.namespace))
	)
	if len(dsts) == 0 {
		prefix := filepath.Join(namespace, key)
		if !strings.HasSuffix(prefix, getSeparator()) && r.enableCacheKeySeparator {
			prefix = prefix + getSeparator()
		}
		entries, err := r.s.List(prefix)

		if err == nil {
			if r.failIfKeyNotPresent && len(entries) == 0 {
				return fmt.Errorf("key %s does not exist", prefix)
			}
			if r.backend == "harness" {
				prefix = r.accountID + "/intel/" + prefix
			}

			for _, e := range entries {
				if r.enableCacheKeySeparator {
					dsts = append(dsts, strings.TrimPrefix(e.Path, prefix))
				} else {
					dsts = append(dsts, strings.TrimPrefix(e.Path, prefix+getSeparator()))
				}
			}
		} else if err != common.ErrNotImplemented {
			return err
		}
	}

	for _, dst := range dsts {
		src := filepath.Join(namespace, key, dst)

		level.Info(r.logger).Log("msg", "restoring directory", "local", dst, "remote", src)
		level.Debug(r.logger).Log("msg", "restoring directory", "remote", src)

		wg.Add(1)

		go func(src, dst string) {
			defer wg.Done()

			if err := r.restore(src, dst); err != nil {
				errs.Add(fmt.Errorf("download from <%s> to <%s>, %w", src, dst, err))
			}
		}(src, dst)
	}

	wg.Wait()

	if errs.Err() != nil {
		return fmt.Errorf("restore failed, %w", errs)
	}

	level.Info(r.logger).Log("msg", "cache restored", "took", time.Since(now))

	return nil
}

// restore fetches the archived file from the cache and restores to the host machine's file system.
func (r restorer) restore(src, dst string) (err error) {
	pr, pw := io.Pipe()
	defer internal.CloseWithErrCapturef(&err, pr, "rebuild, pr close <%s>", dst)

	go func() {
		defer internal.CloseWithErrLogf(r.logger, pw, "pw close defer")

		level.Debug(r.logger).Log("msg", "downloading archived directory", "remote", src, "local", dst)

		if err := r.s.Get(src, pw); err != nil {
			if err := pw.CloseWithError(fmt.Errorf("get file from storage backend, pipe writer failed, %w", err)); err != nil {
				level.Error(r.logger).Log("msg", "pw close", "err", err)
			}
		}
	}()

	level.Debug(r.logger).Log("msg", "extracting archived directory", "remote", src, "local", dst)

	written, err := r.a.Extract(dst, pr)
	if err != nil {
		err = fmt.Errorf("extract files from downloaded archive, pipe reader failed, %w", err)
		if err := pr.CloseWithError(err); err != nil {
			level.Error(r.logger).Log("msg", "pr close", "err", err)
		}

		return err
	}

	writeCacheMetadata(CacheMetadata{CacheSize: humanize.Bytes(uint64(written))}, PLUGIN_CACHE_INTEL_FILE_NAME)

	level.Info(r.logger).Log("msg", "downloaded to local", "directory", dst, "cache size", humanize.Bytes(uint64(written)))

	level.Debug(r.logger).Log(
		"msg", "archive extracted",
		"local", dst,
		"remote", src,
		"raw size", written,
	)

	return nil
}

// Helpers

func (r restorer) generateKey(parts ...string) (string, error) {
	key, err := r.g.Generate(parts...)
	if err == nil {
		return key, nil
	}

	if r.fg != nil {
		level.Error(r.logger).Log("msg", "falling back to fallback key generator", "err", err)

		key, err = r.fg.Generate(parts...)
		if err == nil {
			return key, nil
		}
	}

	return "", err
}

func getSeparator() string {
	if runtime.GOOS == "windows" {
		return `\`
	}

	return "/"
}

func writeCacheMetadata(data CacheMetadata, filename string) error {
	b, err := json.MarshalIndent(data, "", "\t")
	if err != nil {
		return fmt.Errorf("failed with err %s to marshal output %+v", err, data)
	}

	dir := filepath.Dir(filename)
	err = os.MkdirAll(dir, 0644)
	if err != nil {
		return fmt.Errorf("failed with err %s to create %s directory for cache metrics file", err, dir)
	}

	err = os.WriteFile(filename, b, 0644)
	if err != nil {
		return fmt.Errorf("failed to write cache metrics to cache metrics file %s", filename)
	}
	fmt.Println("Successfully wrote to CacheMetadata file at", filename)
	return nil
}
