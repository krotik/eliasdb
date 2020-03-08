export NAME=eliasdb
export TAG=`git describe --abbrev=0 --tags`
export CGO_ENABLED=0
export GOOS=linux

all: build
clean:
	rm -f eliasdb

mod:
	go mod init || true
	go mod tidy
test:
	go test -p 1 ./...
cover:
	go test -p 1 --coverprofile=coverage.out ./...
	go tool cover --html=coverage.out -o coverage.html
	sh -c "open coverage.html || xdg-open coverage.html" 2>/dev/null
fmt:
	gofmt -l -w -s .

vet:
	go vet ./...

build: clean mod fmt vet
	go build -o $(NAME) cli/eliasdb.go

build-win: clean mod fmt vet
	GOOS=windows GOARCH=amd64 go build -o $(NAME).exe cli/eliasdb.go

dist: build build-win
	rm -fR dist

	mkdir -p dist/$(NAME)_linux_amd64
	mv $(NAME) dist/$(NAME)_linux_amd64
	cp LICENSE dist/$(NAME)_linux_amd64
	cp NOTICE dist/$(NAME)_linux_amd64
	tar --directory=dist -cz $(NAME)_linux_amd64 > dist/$(NAME)_$(TAG)_linux_amd64.tar.gz

	mkdir -p dist/$(NAME)_windows_amd64
	mv $(NAME).exe dist/$(NAME)_windows_amd64
	cp LICENSE dist/$(NAME)_windows_amd64
	cp NOTICE dist/$(NAME)_windows_amd64
	tar --directory=dist -cz $(NAME)_windows_amd64 > dist/$(NAME)_$(TAG)_windows_amd64.tar.gz

	sh -c 'cd dist; sha256sum *.tar.gz' > dist/checksums.txt
