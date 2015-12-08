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
}

type keepMessage interface {
	Path() string
}

type requestKeepMessage struct {
	path     string
	fetching bool
}

func (rkm *requestKeepMessage) Path() string {
	return rkm.path
}

type fetchedKeepMessage struct {
	path string
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

func (k *keep) sendRequestMessage(path string, fetching bool) {
	msg := requestKeepMessage{path: path, fetching: fetching}
	k.messageChannel <- &msg
}

func (k *keep) sendFetchedMessage(path string) {
	msg := fetchedKeepMessage{path: path}
	k.messageChannel <- &msg
}

func (k *keep) fetch(path string, otherWriter io.Writer) {
	// If we don't do this, a request error will lead to
	// the entry always being in fetching state, but it won't
	// ever actually be fetched again.
	defer k.sendFetchedMessage(path)

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

	go func() {
		err = mc.Set(&memcache.Item{Key: path, Value: buffer.Bytes()})
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
				if msg.fetching {
					e.fetching = true
				}
			case *fetchedKeepMessage:
				e.lastFetched = time.Now()
				e.fetching = false
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

	item, err := mc.Get(path)
	if err == nil {
		fmt.Printf("found in cache\n")
		_, err = w.Write(item.Value)
		if err != nil {
			fmt.Printf("write error")
			return
		}
		theKeep.sendRequestMessage(path, false)
	} else {
		fmt.Printf("not in cache - requesting\n")
		theKeep.sendRequestMessage(path, true)
		theKeep.fetch(path, w)
	}
}

func main() {
	theKeep = &keep{entries: make(map[string]*entry), messageChannel: make(chan keepMessage)}
	go theKeep.run()

	mc = memcache.New("localhost:11211")

	http.HandleFunc("/", handler)
	http.ListenAndServe(":8081", nil)
}
