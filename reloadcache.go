package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/nytimes/gziphandler"
	"github.com/schani/reloadcache/keep"
)

type memcacheCache struct {
	c      *memcache.Client
	server string
}

var theKeep *keep.Keep
var theMemcache *memcache.Client

func (c memcacheCache) Fetch(path string) (io.ReadCloser, error) {
	req, err := http.NewRequest("GET", c.server+path, nil)
	if err != nil {
		fmt.Printf("request construction error\n")
		return nil, err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Printf("request error\n")
		return nil, err
	}

	if strings.Split(resp.Header.Get("Content-Type"), ";")[0] != "application/json" {
		fmt.Printf("not JSON: %s\n", resp.Header)
		return nil, errors.New("Endpoint does not return JSON")
	}

	return resp.Body, nil
}

func (c memcacheCache) Set(path string, data []byte) error {
	return c.c.Set(&memcache.Item{Key: path, Value: data})
}

func (c memcacheCache) Delete(path string) error {
	return c.c.Delete(path)
}

func cacheHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "" && r.Method != "GET" {
		http.Error(w, "Only GET method supported", http.StatusBadRequest)
		return
	}

	path := r.URL.Path
	if r.URL.RawQuery != "" {
		path = path + "?" + r.URL.RawQuery
	}
	fmt.Printf("request for %s\n", path)
	theKeep.PathRequested(path)

	w.Header().Set("Content-Type", "application/json; charset=UTF-8")

	origin := r.Header.Get("Origin")
	if origin != "" {
		w.Header().Set("Access-Control-Allow-Origin", origin)
	}

	var data []byte
	item, err := theMemcache.Get(path)
	if err == nil {
		fmt.Printf("found in cache %s\n", path)
		data = item.Value
	} else {
		fmt.Printf("not in cache - requesting %s\n", path)

		writerMade := false
		data, err = theKeep.WaitOrFetch(path, func(cacheWriter io.Writer) io.Writer {
			writerMade = true
			w.WriteHeader(http.StatusOK)
			return io.MultiWriter(w, cacheWriter)
		})
		if err != nil {
			if !writerMade {
				http.Error(w, err.Error(), http.StatusBadRequest)
			}
			return
		}
	}

	_, err = w.Write(data)
	if err != nil {
		fmt.Printf("write error")
		return
	}
}

type entryInfos []keep.EntryInfo

func (infos entryInfos) Len() int {
	return len(infos)
}

func (infos entryInfos) Less(i, j int) bool {
	return infos[i].Path < infos[j].Path
}

func (infos entryInfos) Swap(i, j int) {
	tmp := infos[i]
	infos[i] = infos[j]
	infos[j] = tmp
}

func keepHandler(w http.ResponseWriter, r *http.Request) {
	infos := entryInfos(theKeep.Dump())
	sort.Sort(infos)

	w.Header().Set("Content-Type", "text/html; charset=UTF-8")

	fmt.Fprintf(w, "<html><body><table>\n")
	fmt.Fprintf(w, "<tr><th>Path</th><th>Count</th><th>Last fetched</th><th># expires since last decay</th><th>Fetching?</th></tr>")
	for _, ei := range infos {
		var fetchingString string
		if ei.Fetching {
			fetchingString = "Yes"
		} else {
			fetchingString = "No"
		}
		fmt.Fprintf(w, "<tr><td>%s</td><td>%d</td><td>%s</td><td>%d</td><td>%s</td></tr>\n",
			ei.Path, ei.Count, ei.LastFetched, ei.NumExpiredSinceLastDecay, fetchingString)
	}
	fmt.Fprintf(w, "</table></body></html>\n")
}

func main() {
	memcacheFlag := flag.String("memcache", "localhost:11211", "memcached host and port")
	serverFlag := flag.String("server", "", "the proxed server")
	portFlag := flag.Int("port", 8081, "port on which to listen")
	expireDurationFlag := flag.Int("expire", 600, "expire duration in seconds")
	numExpiresToDecayFlag := flag.Int("decay", 5, "number of expires for one decay")

	flag.Parse()

	if *serverFlag == "" {
		fmt.Fprintf(os.Stderr, "Error: -server option not given.\n")
		os.Exit(1)
	}

	theMemcache = memcache.New(*memcacheFlag)
	cache := memcacheCache{c: theMemcache, server: *serverFlag}
	err := cache.c.DeleteAll()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Couldn't flush memcache: %s\n", err.Error())
	}

	theKeep = keep.NewKeep(cache, time.Duration(*expireDurationFlag)*time.Second, *numExpiresToDecayFlag)
	go theKeep.Run()

	http.Handle("/", gziphandler.GzipHandler(http.HandlerFunc(cacheHandler)))
	http.HandleFunc("/admin/keep", keepHandler)
	err = http.ListenAndServe(fmt.Sprintf(":%d", *portFlag), nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: Listen failed: %s\n", err.Error())
		os.Exit(1)
	}
}
