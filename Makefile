all: help

help:
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST) | sort

re: install test

install: ## install binaries
	clear
	go install ./...

test: ## test
	go test -timeout 10s ./...

bench:
	go test -bench=. -count 6 | tee newbench.txt
	benchstat bench.txt newbench.txt | tee benchstat.txt

format:
	mdformat README.md

doc: format
	pkgsite --http localhost:8086
