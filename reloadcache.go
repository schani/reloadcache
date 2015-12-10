package main

import (
	"fmt"
	"github.com/bradfitz/gomemcache/memcache"
	"net/http"
	"os"
)

type memcacheCache struct {
	c *memcache.Client
}

var theKeep *keep

func (c memcacheCache) Get(path string) (data []byte, err error) {
	item, err := c.c.Get(path)
	if err != nil {
		return nil, err
	}
	return item.Value, nil
}

func (c memcacheCache) Set(path string, data []byte) error {
	return c.c.Set(&memcache.Item{Key: path, Value: data})
}

func cacheHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "" && r.Method != "GET" {
		http.Error(w, "Only GET method supported", http.StatusBadRequest)
		return
	}

	path := r.URL.Path
	fmt.Printf("request for %s\n", path)
	theKeep.sendRequestMessage(path)

	w.Header().Set("Content-Type", "application/json; charset=UTF-8")

	data, err := theKeep.cache.Get(path)
	if err == nil {
		fmt.Printf("found in cache\n")
	} else {
		fmt.Printf("not in cache - requesting\n")

		waiter := make(chan []byte)
		theKeep.sendFetchingMessage(path, waiter)

		dataFromOtherFetch, ok := <-waiter
		if ok {
			fmt.Printf("got data from parallel fetch\n")
			data = dataFromOtherFetch
		} else {
			theKeep.fetch(path, w)
			return
		}
	}

	_, err = w.Write(data)
	if err != nil {
		fmt.Printf("write error")
		return
	}
}

func main() {
	cache := memcacheCache{c: memcache.New("localhost:11211")}
	theKeep = newKeep(cache)
	go theKeep.run()

	http.HandleFunc("/", cacheHandler)
	err := http.ListenAndServe(":8081", nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: Listen failed: %s\n", err.Error())
		os.Exit(1)
	}
}
