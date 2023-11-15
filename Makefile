BINARY_NAME=repl

build:
	go build -o bin/$(BINARY_NAME) cmd/$(BINARY_NAME)/main.go
