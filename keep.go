package main

import (
	"bytes"
	"fmt"
	"io"
	// FIXME: Keep shouldn't do HTTP
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

type entry struct {
	info entryInfo
	// Each waiter is a channel waiting for a byte slice.
	// If the fetch fails we close the channel.
	waiters []chan<- []byte
}

type keepMessage interface {
	Process(k *keep)
}

type requestKeepMessage struct {
	path string
}

type fetchingKeepMessage struct {
	path   string
	waiter chan<- []byte
}

type fetchedKeepMessage struct {
	path string
	data *[]byte
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
}

func (k *keep) sendRequestMessage(path string) {
	msg := requestKeepMessage{path: path}
	k.messageChannel <- &msg
}

func (k *keep) sendFetchingMessage(path string, waiter chan<- []byte) {
	msg := fetchingKeepMessage{path: path, waiter: waiter}
	k.messageChannel <- &msg
}

func (k *keep) sendFetchedMessage(path string, data *[]byte) {
	msg := fetchedKeepMessage{path: path, data: data}
	k.messageChannel <- &msg
}

func (k *keep) fetch(path string, responseWriter http.ResponseWriter) error {
	var dataPtr *[]byte
	// If we don't do this, a request error will lead to
	// the entry always being in fetching state, but it won't
	// ever actually be fetched again.
	defer func() { k.sendFetchedMessage(path, dataPtr) }()

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
			http.Error(responseWriter, "Endpoint does not return JSON", http.StatusBadRequest)
		}
		fmt.Printf("not JSON\n")
		return nil
	}

	buffer := new(bytes.Buffer)
	var writer io.Writer
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

	data := buffer.Bytes()
	dataPtr = &data

	go func() {
		err = k.cache.Set(path, data)
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
		if e.info.lastFetched.Add(expireDuration).Before(now) {
			fmt.Printf("fetching %s\n", e.info.path)
			e.info.fetching = true
			go k.fetch(e.info.path, nil)
		}
	}
}

const expireDuration time.Duration = time.Duration(10) * time.Second

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
	expires := earliest.Add(expireDuration)
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
	if msg.data == nil {
		fmt.Printf("no data fetched\n")
	}
	for _, waiter := range e.waiters {
		if msg.data != nil {
			fmt.Printf("satisfying waiter\n")
			waiter <- *msg.data
		} else {
			close(waiter)
		}
	}
	e.waiters = e.waiters[0:0]
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

func newKeep(c cache) *keep {
	return &keep{cache: c, entries: make(map[string]*entry), messageChannel: make(chan keepMessage)}
}
