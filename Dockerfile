FROM alpine:3.3
MAINTAINER Mark Probst "mark@xamarin.com"
EXPOSE 8081

WORKDIR /app

RUN apk add --update memcached && rm -rf /var/cache/apk/*

# copy binary into image
COPY run.sh /app/
COPY reloadcache /app/

ENTRYPOINT ["/bin/sh", "./run.sh"]
