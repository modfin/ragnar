FROM golang:alpine AS builder
RUN mkdir /app
COPY . /app
RUN cd /app/cmd/ragnard && go build -o /ragnard .

FROM alpine:latest
RUN apk --no-cache add ca-certificates pandoc
COPY --from=builder /ragnard /ragnard

ENTRYPOINT ["/ragnard"]