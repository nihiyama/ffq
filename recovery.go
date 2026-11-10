package ffq

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// This state is reconstructed on every open; no generations are stored on disk.
type recoveryPage struct {
	page, start, count uint64
	modified           time.Time
	lines              [][]byte
}

func recoveryError(page uint64, operation string) error {
	// A custom decoder may embed the entire message in its error or panic value.
	return fmt.Errorf("initialize queue: page %d: %s", page, operation)
}

func (q *Queue[T]) recoveryCanceled() error {
	if err := q.currentFailure(); err != nil {
		return err
	}
	select {
	case <-q.stop:
		return ErrQueueClose
	default:
		return nil
	}
}

func (q *Queue[T]) initialize() {
	defer func() {
		if recover() != nil {
			q.initErr = fmt.Errorf("initialize queue: recovery aborted")
		}
		if q.initErr != nil {
			if !IsErrQueueClose(q.initErr) {
				q.fail(q.initErr)
			}
			q.queueCloseOnce.Do(func() { close(q.queue) })
		} else {
			q.ready.Store(true)
		}
		close(q.initializeBlock)
		if q.group != nil && (q.Length() > 0 || q.initErr != nil) {
			q.group.signalEnqueue()
		}
	}()
	q.initErr = q.recoverPages()
}

func (q *Queue[T]) scanPages() ([]*recoveryPage, error) {
	entries, err := os.ReadDir(q.fileDir)
	if err != nil {
		return nil, fmt.Errorf("initialize queue: cannot list directory")
	}
	var pages []*recoveryPage
	for _, entry := range entries {
		if err := q.recoveryCanceled(); err != nil {
			return nil, err
		}
		if !strings.HasPrefix(entry.Name(), queueFilename+".") {
			continue
		}
		page, err := strconv.ParseUint(strings.TrimPrefix(entry.Name(), queueFilename+"."), 10, 64)
		if err != nil || page >= q.maxPage || entry.Name() != fmt.Sprintf("queue.%d", page) {
			return nil, fmt.Errorf("initialize queue: unexpected queue page")
		}
		path := filepath.Join(q.fileDir, entry.Name())
		stat, err := os.Stat(path)
		if err != nil || !stat.Mode().IsRegular() {
			return nil, recoveryError(page, "not a readable regular file")
		}
		f, err := os.Open(path)
		if err != nil {
			return nil, recoveryError(page, "cannot open data")
		}
		p := &recoveryPage{page: page, modified: stat.ModTime()}
		err = func() error {
			defer f.Close()
			reader := bufio.NewReader(f)
			for {
				if err := q.recoveryCanceled(); err != nil {
					return err
				}
				line, e := reader.ReadBytes('\n')
				if e != nil && e != io.EOF {
					return recoveryError(page, "cannot read data")
				}
				if len(line) != 0 {
					n, e := countJSONArrayItems(line)
					if e != nil || n == 0 || n > q.size-p.count {
						return recoveryError(page, "invalid JSON record or page length")
					}
					p.count += n
					p.lines = append(p.lines, line)
				}
				if e == io.EOF {
					return nil
				}
			}
		}()
		if err != nil {
			return nil, err
		}
		pages = append(pages, p)
	}
	return pages, nil
}

// Infer only what the existing contents/timestamps can establish. At a full
// cycle, equal timestamps cannot distinguish completed data from a new cycle.
func (q *Queue[T]) recoveryPositions(pages []*recoveryPage) (uint64, uint64, error) {
	capacity := q.size * q.maxPage
	var newest, partial *recoveryPage
	for _, p := range pages {
		if p.count == 0 {
			continue
		}
		if p.count < q.size {
			if partial != nil {
				return 0, 0, recoveryError(p.page, "multiple partial pages")
			}
			partial = p
		}
		if newest == nil || p.modified.After(newest.modified) {
			newest = p
		}
	}
	if newest == nil {
		if q.initialIndex != nil {
			return 0, 0, fmt.Errorf("initialize queue: index without data")
		}
		return 0, 0, nil
	}
	if q.initialIndex == nil {
		for _, p := range pages {
			if p.count != 0 && p.page > newest.page {
				newest = p
			}
		}
	} else if partial != nil {
		newest = partial
	} else {
		for _, p := range pages {
			if p != newest && p.count != 0 && p.modified.Equal(newest.modified) {
				return 0, 0, recoveryError(p.page, "ambiguous full-page timestamps")
			}
		}
	}
	wrapped := false
	for _, p := range pages {
		if p.count != 0 && p.page > newest.page {
			wrapped = true
		}
	}
	for _, p := range pages {
		p.start = p.page * q.size
		if wrapped && p.page <= newest.page {
			p.start += capacity
		}
	}
	head := newest.start + newest.count
	if q.initialIndex == nil {
		return 0, head, nil
	}
	next := *q.initialIndex + 1
	// A completed cycle can be followed by new data ending partway through
	// the physical last page. There is then no higher page to mark a wrap,
	// so express the next position in this cycle's coordinates. A previous
	// full cycle requires all retained pages; do not accept missing data.
	if next == capacity && head < capacity && newest.page == q.maxPage-1 && uint64(len(pages)) == q.maxPage {
		for _, p := range pages {
			if p.count == 0 {
				return 0, 0, fmt.Errorf("initialize queue: index beyond data")
			}
		}
		next = 0
	}
	if next > head {
		return 0, 0, fmt.Errorf("initialize queue: index beyond data")
	}
	next += (head - next) / capacity * capacity
	if next == head && head >= capacity {
		stat, err := q.indexFile.Stat()
		if err != nil {
			return 0, 0, fmt.Errorf("initialize queue: cannot inspect index")
		}
		if stat.ModTime().Equal(newest.modified) {
			return 0, 0, recoveryError(newest.page, "ambiguous completion timestamp")
		}
		if stat.ModTime().Before(newest.modified) {
			next -= capacity
		}
	}
	if head-next > capacity {
		return 0, 0, fmt.Errorf("initialize queue: progress outside retained data")
	}
	return next, head, nil
}

func (q *Queue[T]) recoverPages() error {
	pages, err := q.scanPages()
	if err != nil {
		return err
	}
	next, head, err := q.recoveryPositions(pages)
	if err != nil {
		return err
	}
	sort.Slice(pages, func(i, j int) bool { return pages[i].start < pages[j].start })
	// Complete validation precedes delivery, preventing a later corrupt record
	// from causing only a prefix of recovery to become acknowledged.
	var messages []*Message[T]
	cursor := next
	for _, p := range pages {
		q.pageEnds[p.page], q.pagePresent[p.page] = p.start+p.count, p.count != 0
		if p.count == 0 || p.start+p.count <= next {
			continue
		}
		if p.start > cursor {
			return recoveryError(p.page, "missing pending page")
		}
		var offset uint64
		for _, line := range p.lines {
			if err := q.recoveryCanceled(); err != nil {
				return err
			}
			n, _ := countJSONArrayItems(line)
			if p.start+offset+n <= next {
				offset += n
				continue
			}
			var items []*T
			if err := q.decoder(line, &items); err != nil || uint64(len(items)) != n {
				return recoveryError(p.page, "cannot decode record")
			}
			for _, item := range items {
				position := p.start + offset
				if position >= next {
					if position != cursor {
						return recoveryError(p.page, "noncontiguous pending data")
					}
					messages = append(messages, &Message[T]{index: position % (q.size * q.maxPage), sequence: position, owner: q, item: item})
					cursor++
				}
				offset++
			}
		}
	}
	if cursor != head {
		return fmt.Errorf("initialize queue: missing pending data")
	}
	if err := q.recoveryCanceled(); err != nil {
		return err
	}
	page := (head % (q.size * q.maxPage)) / q.size
	var writer *recoveryPage
	for _, p := range pages {
		if p.page == page {
			writer = p
			break
		}
	}
	// Never truncate or delete on open. If a full page must be reused, defer
	// that operation until enqueue can wait for persisted consumer progress.
	f, err := os.OpenFile(filepath.Join(q.fileDir, fmt.Sprintf("queue.%d", page)), fOpenFlag, 0644)
	if err != nil {
		return recoveryError(page, "cannot open writer")
	}
	position, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		f.Close()
		return recoveryError(page, "cannot locate writer")
	}
	q.writeOffset = position
	q.rotatePending = head%q.size == 0 && writer != nil && writer.count != 0
	if writer != nil && len(writer.lines) > 0 {
		last := writer.lines[len(writer.lines)-1]
		q.writeNeedsLF = last[len(last)-1] != '\n'
	}
	q.queueFile.Store(f)
	atomic.StoreUint64(&q.currentPage, page)
	atomic.StoreUint64(&q.tail, head%(q.size*q.maxPage))
	q.sequence = head
	q.ackNext.Store(next)
	for _, m := range messages {
		if err := q.publish(m); err != nil {
			return err
		}
	}
	return nil
}
