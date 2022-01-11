.PHONY: all
all: validate test clean

## Run validates
.PHONY: validate
validate:
	golangci-lint run

PACKAGES=$(shell go list ./store/...)

## Run tests
.PHONY: test
test: test-start-stack
test: ${PACKAGES}

## Run tests for ${PACKAGES}
## Example: make github.com/kvtools/valkeyrie/store/redis
.PHONY: ${PACKAGES}
${PACKAGES}:
	go test -v -race ${TEST_ARGS} $@

## Launch docker stack for test
.PHONY: test-start-stack
test-start-stack:
	docker-compose -f script/docker-compose.yml up --wait

## Clean local data
.PHONY: clean
clean:
	docker-compose -f script/docker-compose.yml down
	$(RM) goverage.report $(shell find . -type f -name *.out)
