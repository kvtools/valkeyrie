.PHONY: all
all: test clean

PACKAGES=$(shell go list ./store/...)

## Run tests
.PHONY: test
test: test-start-stack
test: ${PACKAGES}

## Run tests for ${PACKAGES}
## Example: make github.com/kvtools/valkeyrie/store/redis
.PHONY: ${PACKAGES}
${PACKAGES}:
	go test -v -race $@

## Launch docker stack for test
.PHONY: test-start-stack
test-start-stack:
	docker-compose -f script/docker-compose.yml up -d
	@timeout 60s ./script/wait-stack

## Clean local data
.PHONY: clean
clean:
	docker-compose -f script/docker-compose.yml down
	$(RM) goverage.report $(shell find . -type f -name *.out)
