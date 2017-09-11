all: | fmt install

fmt:
	go fmt

install:
	go install -v

.PHONY: fmt install
