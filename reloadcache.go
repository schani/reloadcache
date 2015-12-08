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

type keepRequest struct {
	path     string
	fetching bool
	fetched  bool
}

type keep struct {
	entries        map[string]*entry
	timer          *time.Timer
	requestChannel chan keepRequest
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

func (k *keep) sendRequest(path string, fetching bool, fetched bool) {
	req := keepRequest{path: path, fetching: fetching, fetched: fetched}
	k.requestChannel <- req
}

func (k *keep) fetch(path string, otherWriter io.Writer) {
	// If we don't do this, a request error will lead to
	// the entry always being in fetching state, but it won't
	// ever actually be fetched again.
	defer k.sendRequest(path, false, true)

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
		case req := <-k.requestChannel:
			e, ok := k.entries[req.path]
			if !ok {
				e = &entry{path: req.path, count: 0}
			}
			if !ok || req.fetched {
				e.lastFetched = time.Now()
			}
			if req.fetching {
				e.fetching = true
			}
			if req.fetched {
				e.fetching = false
			} else {
				e.count++
			}
			k.entries[req.path] = e
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
		theKeep.sendRequest(path, false, false)
	} else {
		fmt.Printf("not in cache - requesting\n")
		theKeep.sendRequest(path, true, false)
		theKeep.fetch(path, w)
	}
}

func main() {
	theKeep = &keep{entries: make(map[string]*entry), requestChannel: make(chan keepRequest)}
	go theKeep.run()

	mc = memcache.New("localhost:11211")

	http.HandleFunc("/", handler)
	http.ListenAndServe(":8081", nil)
}
