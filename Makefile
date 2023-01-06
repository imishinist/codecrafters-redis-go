
.PHONY: test
test: install-tester
	CODECRAFTERS_SUBMISSION_DIR=$(shell pwd) CODECRAFTERS_CURRENT_STAGE_SLUG='expiry' redis-tester

.PHONY: install-tester
install-tester:
	go install github.com/codecrafters-io/redis-tester/cmd/tester@latest
	@mv $(HOME)/go/bin/tester $(HOME)/go/bin/redis-tester
