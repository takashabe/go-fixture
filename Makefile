.PHONY: test deps

test:
	go test -v -p 1 ./...

deps:
	dep ensure
