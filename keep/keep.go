package keep

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

type EntryInfo struct {
	Path                     string
	Count                    int
	LastFetched              time.Time
	Fetching                 bool
	NumExpiredSinceLastDecay int
}

type FetchResult struct {
	Data []byte
	Err  error
}

type entry struct {
	info EntryInfo
	// Each waiter is a channel waiting for a byte slice.
	// If the fetch fails we close the channel.
	waiters []chan<- FetchResult
}

type keepMessage interface {
	process(k *Keep)
}

type requestKeepMessage struct {
	path string
}

type fetchingKeepMessage struct {
	path   string
	waiter chan<- FetchResult
}

type fetchedKeepMessage struct {
	path   string
	result FetchResult
}

type dumpKeepMessage struct {
	channel chan<- EntryInfo
}

type dontReloadKeepMessage struct {
	path string
}

type Cache interface {
	Set(path string, data []byte) error
	Delete(path string) error
}

type Keep struct {
	entries           map[string]*entry
	timer             *time.Timer
	messageChannel    chan keepMessage
	cache             Cache
	server            string
	expireDuration    time.Duration
	numExpiresToDecay int
}

func (k *Keep) sendRequestMessage(path string) {
	msg := requestKeepMessage{path: path}
	k.messageChannel <- &msg
}

func (k *Keep) sendFetchingMessage(path string, waiter chan<- FetchResult) {
	msg := fetchingKeepMessage{path: path, waiter: waiter}
	k.messageChannel <- &msg
}

func (k *Keep) sendFetchedMessage(path string, result FetchResult) {
	msg := fetchedKeepMessage{path: path, result: result}
	k.messageChannel <- &msg
}

func (k *Keep) SendDumpKeepMessage(channel chan<- EntryInfo) {
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

func (k *Keep) TryLookup(path string) (FetchResult, bool) {
    waiter := make(chan FetchResult)
    k.sendFetchingMessage(path, waiter)

    result, ok := <-waiter
    return result, ok
}

type WriterMaker func(w io.Writer) io.Writer

func (k *Keep) Fetch(path string, writerMaker WriterMaker) error {
	var data []byte
	var err error

	// If we don't do this, a request error will lead to
	// the entry always being in fetching state, but it won't
	// ever actually be fetched again.
	defer func() { k.sendFetchedMessage(path, FetchResult{Data: data, Err: err}) }()

	req, err := http.NewRequest("GET", k.server+path, nil)
	if err != nil {
		fmt.Printf("request construction error\n")
		return err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Printf("request error\n")
		return err
	}

	if strings.Split(resp.Header.Get("Content-Type"), ";")[0] != "application/json" {
		fmt.Printf("not JSON: %s\n", resp.Header)
		return errors.New("Endpoint does not return JSON")
	}

	buffer := new(bytes.Buffer)
    writer := writerMaker(buffer)

	_, err = io.Copy(writer, resp.Body)
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
		e.info.NumExpiredSinceLastDecay++
		if e.info.NumExpiredSinceLastDecay >= k.numExpiresToDecay {
			e.info.Count--
			e.info.NumExpiredSinceLastDecay = 0
		}
		if e.info.Count <= 0 {
			fmt.Printf("deleting %s\n", e.info.Path)
			k.cache.Delete(e.info.Path)
			// FIXME: delete entry, too
			continue
		}
		if e.info.LastFetched.Add(k.expireDuration).Before(now) {
			fmt.Printf("fetching %s\n", e.info.Path)
			e.info.Fetching = true
			go k.Fetch(e.info.Path, nil)
		}
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

func (k *Keep) serviceTimer() {
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

func (k *Keep) Run() {
	k.serviceTimer()
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
		k.serviceTimer()
	}
}

func NewKeep(c Cache, server string, expireDuration time.Duration, numExpiresToDecay int) *Keep {
	return &Keep{cache: c,
		server:            server,
		entries:           make(map[string]*entry),
		messageChannel:    make(chan keepMessage),
		expireDuration:    expireDuration,
		numExpiresToDecay: numExpiresToDecay}
}
