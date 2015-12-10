package main

import (
	"fmt"
	"github.com/bradfitz/gomemcache/memcache"
	"net/http"
	"os"
	"sort"
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

		waiter := make(chan fetchResult)
		theKeep.sendFetchingMessage(path, waiter)

		result, ok := <-waiter
		if ok {
			fmt.Printf("got result from parallel fetch\n")
			if result.err != nil {
				http.Error(w, result.err.Error(), http.StatusBadRequest)
				return
			}
			data = result.data
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

type entryInfos []entryInfo

func (infos entryInfos) Len() int {
	return len(infos)
}

func (infos entryInfos) Less(i, j int) bool {
	return infos[i].path < infos[j].path
}

func (infos entryInfos) Swap(i, j int) {
	tmp := infos[i]
	infos[i] = infos[j]
	infos[j] = tmp
}

func keepHandler(w http.ResponseWriter, r *http.Request) {
	c := make(chan entryInfo)
	theKeep.sendDumpKeepMessage(c)

	infos := make(entryInfos, 0)
	for ei := range c {
		infos = append(infos, ei)
	}
	sort.Sort(infos)

	w.Header().Set("Content-Type", "text/html; charset=UTF-8")

	fmt.Fprintf(w, "<html><body><table>\n")
	fmt.Fprintf(w, "<tr><th>Path</th><th>Count</th><th>Last fetched</th><th>Fetching?</th></tr>")
	for _, ei := range infos {
		var fetchingString string
		if ei.fetching {
			fetchingString = "Yes"
		} else {
			fetchingString = "No"
		}
		fmt.Fprintf(w, "<tr><td>%s</td><td>%d</td><td>%s</td><td>%s</td></tr>\n",
			ei.path, ei.count, ei.lastFetched, fetchingString)
	}
	fmt.Fprintf(w, "</table></body></html>\n")
}

func main() {
	cache := memcacheCache{c: memcache.New("localhost:11211")}
	theKeep = newKeep(cache)
	go theKeep.run()

	http.HandleFunc("/", cacheHandler)
	http.HandleFunc("/admin/keep", keepHandler)
	err := http.ListenAndServe(":8081", nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: Listen failed: %s\n", err.Error())
		os.Exit(1)
	}
}
