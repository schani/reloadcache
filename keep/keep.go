package keep

import (
	"bytes"
	"fmt"
	"io"
	"time"
)

type EntryInfo struct {
	Path                     string
	Count                    int
	LastFetched              time.Time
	Fetching                 bool
	NumExpiredSinceLastDecay int
}

type fetchResult struct {
	Data []byte
	Err  error
}

type entry struct {
	info EntryInfo
	// Each waiter is a channel waiting for a byte slice.
	// If the fetch fails we close the channel.
	waiters []chan<- fetchResult
}

type keepMessage interface {
	process(k *Keep)
}

type requestKeepMessage struct {
	path string
}

type fetchingKeepMessage struct {
	path   string
	waiter chan<- fetchResult
}

type fetchedKeepMessage struct {
	path   string
	result fetchResult
}

type dumpKeepMessage struct {
	channel chan<- EntryInfo
}

type dontReloadKeepMessage struct {
	path string
}

type Cache interface {
	Fetch(path string) (io.ReadCloser, error)
	Set(path string, data []byte) error
	Delete(path string) error
}

type Keep struct {
	entries           map[string]*entry
	timer             *time.Timer
	messageChannel    chan keepMessage
	cache             Cache
	expireDuration    time.Duration
	numExpiresToDecay int
}

func (k *Keep) sendRequestMessage(path string) {
	msg := requestKeepMessage{path: path}
	k.messageChannel <- &msg
}

func (k *Keep) sendFetchingMessage(path string, waiter chan<- fetchResult) {
	msg := fetchingKeepMessage{path: path, waiter: waiter}
	k.messageChannel <- &msg
}

func (k *Keep) sendFetchedMessage(path string, result fetchResult) {
	msg := fetchedKeepMessage{path: path, result: result}
	k.messageChannel <- &msg
}

func (k *Keep) sendDumpKeepMessage(channel chan<- EntryInfo) {
	msg := dumpKeepMessage{channel: channel}
	k.messageChannel <- &msg
}

func (k *Keep) sendDontReloadKeepMessage(path string) {
	msg := dontReloadKeepMessage{path: path}
	k.messageChannel <- &msg
}

func (k *Keep) PathRequested(path string) {
	k.sendRequestMessage(path)
}

func (k *Keep) tryLookup(path string) (fetchResult, bool) {
	waiter := make(chan fetchResult)
	k.sendFetchingMessage(path, waiter)

	result, ok := <-waiter
	return result, ok
}

type WriterMaker func(w io.Writer) io.Writer

func (k *Keep) WaitOrFetch(path string, writerMaker WriterMaker) ([]byte, error) {
	result, ok := k.tryLookup(path)
	if ok {
		fmt.Printf("got result from parallel fetch\n")
		if result.Err != nil {
			return nil, result.Err
		}
		return result.Data, nil
	}

	return nil, k.fetch(path, writerMaker)
}

func (k *Keep) fetch(path string, writerMaker WriterMaker) error {
	var data []byte
	var err error

	// If we don't do this, a request error will lead to
	// the entry always being in fetching state, but it won't
	// ever actually be fetched again.
	defer func() { k.sendFetchedMessage(path, fetchResult{Data: data, Err: err}) }()

	resp, err := k.cache.Fetch(path)
	if err != nil {
		return err
	}
	defer resp.Close()

	buffer := new(bytes.Buffer)
	writer := writerMaker(buffer)

	_, err = io.Copy(writer, resp)
	if err != nil {
		fmt.Printf("copy error\n")
		return err
	}

	data = buffer.Bytes()

	go func() {
		err := k.cache.Set(path, data)
		if err != nil {
			fmt.Printf("cache set error\n")
			k.sendDontReloadKeepMessage(path)
		}
	}()

	return nil
}

func (k *Keep) fetchExpired() {
	fmt.Printf("fetching expired\n")
	now := time.Now()
	for _, e := range k.entries {
		if e.info.Fetching || e.info.Count <= 0 {
			continue
		}
		expired := e.info.LastFetched.Add(k.expireDuration).Before(now)
		if expired {
			e.info.NumExpiredSinceLastDecay++
			if e.info.NumExpiredSinceLastDecay >= k.numExpiresToDecay {
				e.info.Count--
				e.info.NumExpiredSinceLastDecay = 0
			}
		}
		if e.info.Count <= 0 {
			fmt.Printf("deleting %s\n", e.info.Path)
			k.cache.Delete(e.info.Path)
			// FIXME: delete entry, too
			continue
		}
		if !expired {
			continue
		}

		fmt.Printf("fetching %s\n", e.info.Path)
		e.info.Fetching = true
		go k.fetch(e.info.Path, func(w io.Writer) io.Writer { return w })
	}
}

func (k *Keep) shortestTimeout() (duration time.Duration, expiring bool) {
	now := time.Now()
	earliest := now
	expiring = false
	for _, e := range k.entries {
		if e.info.Fetching || e.info.Count <= 0 {
			continue
		}
		if e.info.LastFetched.Before(earliest) {
			earliest = e.info.LastFetched
			expiring = true
		}
	}
	expires := earliest.Add(k.expireDuration)
	if expires.Before(now) {
		return 0, expiring
	}
	return expires.Sub(now), expiring
}

func (k *Keep) updateServiceTimer() {
	if k.timer != nil {
		return
	}

	for {
		k.fetchExpired()

		duration, expiring := k.shortestTimeout()
		if !expiring {
			return
		}

		if duration > 0 {
			k.timer = time.NewTimer(duration)
			return
		}
	}
}

func (rkm *requestKeepMessage) process(k *Keep) {
	path := rkm.path

	e, ok := k.entries[path]
	if !ok {
		e = &entry{info: EntryInfo{Path: path, Count: 1, LastFetched: time.Now()}}
		k.entries[path] = e
		return
	}

	e.info.Count++
}

func (msg *fetchingKeepMessage) process(k *Keep) {
	path := msg.path

	e, ok := k.entries[path]
	if !ok {
		panic("Fetching a path that hasn't been requested yet")
	}

	if e.info.Fetching {
		fmt.Printf("adding waiter\n")
		e.waiters = append(e.waiters, msg.waiter)
	} else {
		close(msg.waiter)
		e.info.Fetching = true
	}
}

func (msg *fetchedKeepMessage) process(k *Keep) {
	path := msg.path

	e, ok := k.entries[path]
	if !ok {
		panic("Fetching a path that hasn't been requested yet")
	}

	if !e.info.Fetching {
		panic("We got a fetched, but we're not fetching")
	}

	e.info.LastFetched = time.Now()
	e.info.Fetching = false

	for _, waiter := range e.waiters {
		waiter <- msg.result
		close(waiter)
	}
	e.waiters = e.waiters[0:0]
}

func (msg *dumpKeepMessage) process(k *Keep) {
	for _, e := range k.entries {
		msg.channel <- e.info
	}
	close(msg.channel)
}

func (msg *dontReloadKeepMessage) process(k *Keep) {
	path := msg.path

	e, ok := k.entries[path]
	if !ok {
		panic("Stopping reload on path that doesn't exist")
	}

	e.info.Count = 0
}

// Run runs the keep in an endless loop.  You should probably
// run this in a goroutine.
func (k *Keep) Run() {
	k.updateServiceTimer()
	for {
		var timerChannel <-chan time.Time
		if k.timer != nil {
			timerChannel = k.timer.C
		}
		select {
		case msg := <-k.messageChannel:
			msg.process(k)
		case <-timerChannel:
			k.timer = nil
		}
		k.updateServiceTimer()
	}
}

// Dump returns a slice containing all the entries in the keep.
// They are not sorted.
func (k *Keep) Dump() []EntryInfo {
	c := make(chan EntryInfo)
	k.sendDumpKeepMessage(c)

	var infos []EntryInfo
	for ei := range c {
		infos = append(infos, ei)
	}
	return infos
}

// NewKeep returns a new keep.  expireDuration is the time an entry
// takes to be refetched by the keep.  numExpiresToDecay is the number
// of refetches it takes for the entry count to degrade by one.
func NewKeep(c Cache, expireDuration time.Duration, numExpiresToDecay int) *Keep {
	return &Keep{cache: c,
		entries:           make(map[string]*entry),
		messageChannel:    make(chan keepMessage),
		expireDuration:    expireDuration,
		numExpiresToDecay: numExpiresToDecay}
}
