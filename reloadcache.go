package main

import (
	"bytes"
	"fmt"
	"github.com/bradfitz/gomemcache/memcache"
	"io"
	"net/http"
	"time"
)

var mc *memcache.Client

type entry struct {
	path        string
	count       int
	lastFetched time.Time
	fetching    bool
	// Each waiter is a channel waiting for a byte slice.
	// If the fetch fails we close the channel.
	waiters []chan<- []byte
}

type keepMessage interface {
	Path() string
}

type requestKeepMessage struct {
	path   string
	waiter chan<- []byte
}

func (rkm *requestKeepMessage) Path() string {
	return rkm.path
}

type fetchingKeepMessage struct {
	path string
}

func (fkm *fetchingKeepMessage) Path() string {
	return fkm.path
}

type fetchedKeepMessage struct {
	path string
	data *[]byte
}

func (fkm *fetchedKeepMessage) Path() string {
	return fkm.path
}

type keep struct {
	entries        map[string]*entry
	timer          *time.Timer
	messageChannel chan keepMessage
}

const expireDuration time.Duration = time.Duration(10) * time.Second

func (k *keep) shortestTimeout() (duration time.Duration, expiring bool) {
	now := time.Now()
	earliest := now
	expiring = false
	for _, e := range k.entries {
		if e.fetching {
			continue
		}
		if e.lastFetched.Before(earliest) {
			earliest = e.lastFetched
			expiring = true
		}
	}
	expires := earliest.Add(expireDuration)
	if expires.Before(now) {
		return 0, expiring
	}
	return expires.Sub(now), expiring
}

func (k *keep) sendRequestMessage(path string, waiter chan<- []byte) {
	msg := requestKeepMessage{path: path, waiter: waiter}
	k.messageChannel <- &msg
}

func (k *keep) sendFetchingMessage(path string) {
	msg := fetchingKeepMessage{path: path}
	k.messageChannel <- &msg
}

func (k *keep) sendFetchedMessage(path string, data *[]byte) {
	msg := fetchedKeepMessage{path: path, data: data}
	k.messageChannel <- &msg
}

func (k *keep) fetch(path string, otherWriter io.Writer) {
	var data *[]byte
	// If we don't do this, a request error will lead to
	// the entry always being in fetching state, but it won't
	// ever actually be fetched again.
	defer func() { k.sendFetchedMessage(path, data) }()

	req, err := http.NewRequest("GET", "http://localhost:8080"+path, nil)
	if err != nil {
		fmt.Printf("request construction error\n")
		return
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Printf("request error\n")
		return
	}

	buffer := new(bytes.Buffer)
	var writer io.Writer
	if otherWriter == nil {
		writer = buffer
	} else {
		writer = io.MultiWriter(otherWriter, buffer)
	}

	_, err = io.Copy(writer, resp.Body)
	if err != nil {
		fmt.Printf("copy error\n")
		return
	}

	bytes := buffer.Bytes()
	data = &bytes

	go func() {
		err = mc.Set(&memcache.Item{Key: path, Value: bytes})
		if err != nil {
			fmt.Printf("memcache set error\n")
		}
	}()
}

func (k *keep) fetchExpired() {
	fmt.Printf("fetching expired\n")
	now := time.Now()
	for _, e := range k.entries {
		if e.fetching {
			continue
		}
		if e.lastFetched.Add(expireDuration).Before(now) {
			fmt.Printf("fetching %s\n", e.path)
			e.fetching = true
			go k.fetch(e.path, nil)
		}
	}
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

func (k *keep) run() {
	k.serviceTimer()
	for {
		var timerChannel <-chan time.Time
		if k.timer != nil {
			timerChannel = k.timer.C
		}
		select {
		case msg := <-k.messageChannel:
			path := msg.Path()
			e, ok := k.entries[path]
			if !ok {
				e = &entry{path: path, count: 0}
			}
			if !ok {
				e.lastFetched = time.Now()
			}
			switch msg := msg.(type) {
			case *requestKeepMessage:
				e.count++
				if e.fetching {
					fmt.Printf("adding waiter\n")
					e.waiters = append(e.waiters, msg.waiter)
				} else {
					close(msg.waiter)
				}
			case *fetchingKeepMessage:
				e.fetching = true
			case *fetchedKeepMessage:
				e.lastFetched = time.Now()
				e.fetching = false
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
			k.entries[path] = e
		case <-timerChannel:
			k.timer = nil
		}
		k.serviceTimer()
	}
}

var theKeep *keep

func handler(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	fmt.Printf("request for %s\n", path)

	// FIXME: It should be the fetching message that sends a
	// waiter channel, not the request message.  The way it
	// is now we can end up with more than handler fetching
	// the same URL in parallel.

	waiter := make(chan []byte)
	theKeep.sendRequestMessage(path, waiter)
	data, ok := <-waiter
	if ok {
		fmt.Printf("got data from parallel fetch\n")
	} else {
		item, err := mc.Get(path)
		if err == nil {
			fmt.Printf("found in cache\n")
		} else {
			fmt.Printf("not in cache - requesting\n")
			theKeep.sendFetchingMessage(path)
			theKeep.fetch(path, w)
			return
		}

		data = item.Value
	}

	_, err := w.Write(data)
	if err != nil {
		fmt.Printf("write error")
		return
	}
}

func main() {
	theKeep = &keep{entries: make(map[string]*entry), messageChannel: make(chan keepMessage)}
	go theKeep.run()

	mc = memcache.New("localhost:11211")

	http.HandleFunc("/", handler)
	http.ListenAndServe(":8081", nil)
}
