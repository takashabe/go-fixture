.PHONY: test deps

test:
	go test -v

deps:
	dep ensure
