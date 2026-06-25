.PHONY: run demo test build

run:
	go run ./cmd/server

demo:
	go run ./cmd/demo

test:
	go test ./... -count=1

build:
	go build -o bin/gokube ./cmd/server
	go build -o bin/gokube-demo ./cmd/demo
