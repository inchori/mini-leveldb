# 변수 정의
BINARY_NAME=minildb
BINARY_DIR=build
SOURCE_DIR=cmd/minildb

.PHONY: all
all: build

.PHONY: build
build:
	@mkdir -p $(BINARY_DIR)
	go build -o $(BINARY_DIR)/$(BINARY_NAME) ./$(SOURCE_DIR)

.PHONY: test
test:
	go test -v ./...

.PHONY: run
run: build
	./$(BINARY_DIR)/$(BINARY_NAME)

.PHONY: clean
clean:
	rm -rf $(BINARY_DIR)

.PHONY: install
install:
	go install ./$(SOURCE_DIR)
