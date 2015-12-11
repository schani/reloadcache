package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

type entryInfo struct {
	path        string
	count       int
	lastFetched time.Time
	fetching    bool
}

type fetchResult struct {
	data []byte
	err  error
}

type entry struct {
	info entryInfo
	// Each waiter is a channel waiting for a byte slice.
	// If the fetch fails we close the channel.
	waiters []chan<- fetchResult
}

type keepMessage interface {
	Process(k *keep)
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
	channel chan<- entryInfo
}

type cache interface {
	Get(path string) (data []byte, err error)
	Set(path string, data []byte) error
}

type keep struct {
	entries        map[string]*entry
	timer          *time.Timer
	messageChannel chan keepMessage
	cache          cache
	expireDuration time.Duration
}

func (k *keep) sendRequestMessage(path string) {
	msg := requestKeepMessage{path: path}
	k.messageChannel <- &msg
}

func (k *keep) sendFetchingMessage(path string, waiter chan<- fetchResult) {
	msg := fetchingKeepMessage{path: path, waiter: waiter}
	k.messageChannel <- &msg
}

func (k *keep) sendFetchedMessage(path string, result fetchResult) {
	msg := fetchedKeepMessage{path: path, result: result}
	k.messageChannel <- &msg
}

func (k *keep) sendDumpKeepMessage(channel chan<- entryInfo) {
	msg := dumpKeepMessage{channel: channel}
	k.messageChannel <- &msg
}

func (k *keep) fetch(path string, responseWriter http.ResponseWriter) error {
	var data []byte
	var err error

	// If we don't do this, a request error will lead to
	// the entry always being in fetching state, but it won't
	// ever actually be fetched again.
	defer func() { k.sendFetchedMessage(path, fetchResult{data: data, err: err}) }()

	req, err := http.NewRequest("GET", "http://localhost:8085"+path, nil)
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
		if responseWriter != nil {
			err = errors.New("Endpoint does not result JSON")
			http.Error(responseWriter, err.Error(), http.StatusBadRequest)
		}
		fmt.Printf("not JSON\n")
		return nil
	}

	buffer := new(bytes.Buffer)
	var writer io.Writer

	// FIXME: We shouldn't do HTTP stuff on the response writer here.
	// Best not to assume a responseWriter at all but instead just
	// get a function that takes our writer and returns the writer
	// to actually write to.
	if responseWriter == nil {
		writer = buffer
	} else {
		responseWriter.WriteHeader(http.StatusOK)

		writer = io.MultiWriter(responseWriter, buffer)
	}

	_, err = io.Copy(writer, resp.Body)
	if err != nil {
		fmt.Printf("copy error\n")
		return nil
	}

	data = buffer.Bytes()

	go func() {
		err := k.cache.Set(path, data)
		if err != nil {
			fmt.Printf("cache set error\n")
		}
	}()

	return nil
}

func (k *keep) fetchExpired() {
	fmt.Printf("fetching expired\n")
	now := time.Now()
	for _, e := range k.entries {
		if e.info.fetching {
			continue
		}
		if e.info.lastFetched.Add(k.expireDuration).Before(now) {
			fmt.Printf("fetching %s\n", e.info.path)
			e.info.fetching = true
			go k.fetch(e.info.path, nil)
		}
	}
}

func (k *keep) shortestTimeout() (duration time.Duration, expiring bool) {
	now := time.Now()
	earliest := now
	expiring = false
	for _, e := range k.entries {
		if e.info.fetching {
			continue
		}
		if e.info.lastFetched.Before(earliest) {
			earliest = e.info.lastFetched
			expiring = true
		}
	}
	expires := earliest.Add(k.expireDuration)
	if expires.Before(now) {
		return 0, expiring
	}
	return expires.Sub(now), expiring
}

func (k *keep) serviceTimer() {
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

func (rkm *requestKeepMessage) Process(k *keep) {
	path := rkm.path

	e, ok := k.entries[path]
	if !ok {
		e = &entry{info: entryInfo{path: path, count: 1, lastFetched: time.Now()}}
		k.entries[path] = e
		return
	}

	e.info.count++
}

func (msg *fetchingKeepMessage) Process(k *keep) {
	path := msg.path

	e, ok := k.entries[path]
	if !ok {
		panic("Fetching a path that hasn't been requested yet")
	}

	if e.info.fetching {
		fmt.Printf("adding waiter\n")
		e.waiters = append(e.waiters, msg.waiter)
	} else {
		close(msg.waiter)
		e.info.fetching = true
	}
}

func (msg *fetchedKeepMessage) Process(k *keep) {
	path := msg.path

	e, ok := k.entries[path]
	if !ok {
		panic("Fetching a path that hasn't been requested yet")
	}

	if !e.info.fetching {
		panic("We got a fetched, but we're not fetching")
	}

	e.info.lastFetched = time.Now()
	e.info.fetching = false

	for _, waiter := range e.waiters {
		waiter <- msg.result
		close(waiter)
	}
	e.waiters = e.waiters[0:0]
}

func (msg *dumpKeepMessage) Process(k *keep) {
	for _, e := range k.entries {
		msg.channel <- e.info
	}
	close(msg.channel)
}

func (k *keep) run() {
	k.serviceTimer()
	for {
		var timerChannel <-chan time.Time
		if k.timer != nil {
			timerChannel = k.timer.C
		}
		select {
		case msg := <-k.messageChannel:
			msg.Process(k)
		case <-timerChannel:
			k.timer = nil
		}
		k.serviceTimer()
	}
}

func newKeep(c cache, expireDuration time.Duration) *keep {
	return &keep{cache: c,
		entries: make(map[string]*entry),
		messageChannel: make(chan keepMessage),
		expireDuration: expireDuration}
}
