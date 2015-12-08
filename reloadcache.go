package main

import (
	"fmt"
	"net/http"
	"io"
	"bytes"
	"github.com/bradfitz/gomemcache/memcache"
)

var mc *memcache.Client

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
	} else {
		fmt.Printf("not in cache - requesting\n")
		req, err := http.NewRequest("GET", "http://localhost:8080" + path, nil)
		if err != nil {
			fmt.Printf("request construction error")
			return
		}
		
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			fmt.Printf("request error")
			return
		}

		buffer := new(bytes.Buffer) 
		multiwriter := io.MultiWriter(w, buffer)
		
		_, err = io.Copy(multiwriter, resp.Body)
		if err != nil {
			fmt.Printf("copy error")
			return
		}
		
		go func() {
			err = mc.Set(&memcache.Item{Key: path, Value: buffer.Bytes()})
			if err != nil {
				fmt.Printf("memcache set error")
			}
		} ()
	}
}

func main() {
	mc = memcache.New("localhost:11211")

	http.HandleFunc("/", handler)
	http.ListenAndServe(":8081", nil)
}
