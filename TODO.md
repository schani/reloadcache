# Can we store gzipped data in memcache? 

Right now we get gzipped data from PostgREST (do we, actually?),
dezip it, store the result in memcache, then zip it again to send
it to the client.  We use more processing power and more memory.
