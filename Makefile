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

test-mac:
	GOOS=darwin GOARCH=amd64 go test -p 1 ./...

cover:
	go test -p 1 --coverprofile=coverage.out ./...
	go tool cover --html=coverage.out -o coverage.html
	sh -c "open coverage.html || xdg-open coverage.html" 2>/dev/null

fmt:
	gofmt -l -w -s .

vet:
	go vet ./...

build: clean mod fmt vet
	go build -ldflags "-s -w" -o $(NAME) cli/eliasdb.go

build-mac: clean mod fmt vet
	GOOS=darwin GOARCH=amd64 go build -o $(NAME).mac cli/eliasdb.go

build-win: clean mod fmt vet
	GOOS=windows GOARCH=amd64 go build -o $(NAME).exe cli/eliasdb.go

build-arm7: clean mod fmt vet
	GOOS=linux GOARCH=arm GOARM=7 go build -o $(NAME).arm7 cli/eliasdb.go

build-arm8: clean mod fmt vet
	GOOS=linux GOARCH=arm64 go build -o $(NAME).arm8 cli/eliasdb.go

dist: build build-win build-mac build-arm7 build-arm8
	rm -fR dist

	mkdir -p dist/$(NAME)_linux_amd64
	mv $(NAME) dist/$(NAME)_linux_amd64
	cp -fR examples dist/$(NAME)_linux_amd64
	cp LICENSE dist/$(NAME)_linux_amd64
	cp NOTICE dist/$(NAME)_linux_amd64
	tar --directory=dist -cz $(NAME)_linux_amd64 > dist/$(NAME)_$(TAG)_linux_amd64.tar.gz

	mkdir -p dist/$(NAME)_darwin_amd64
	mv $(NAME).mac dist/$(NAME)_darwin_amd64/$(NAME)
	cp -fR examples dist/$(NAME)_darwin_amd64
	cp LICENSE dist/$(NAME)_darwin_amd64
	cp NOTICE dist/$(NAME)_darwin_amd64
	tar --directory=dist -cz $(NAME)_darwin_amd64 > dist/$(NAME)_$(TAG)_darwin_amd64.tar.gz

	mkdir -p dist/$(NAME)_windows_amd64
	mv $(NAME).exe dist/$(NAME)_windows_amd64
	cp -fR examples dist/$(NAME)_windows_amd64
	cp LICENSE dist/$(NAME)_windows_amd64
	cp NOTICE dist/$(NAME)_windows_amd64
	tar --directory=dist -cz $(NAME)_windows_amd64 > dist/$(NAME)_$(TAG)_windows_amd64.tar.gz

	mkdir -p dist/$(NAME)_arm7
	mv $(NAME).arm7 dist/$(NAME)_arm7
	cp -fR examples dist/$(NAME)_arm7
	cp LICENSE dist/$(NAME)_arm7
	cp NOTICE dist/$(NAME)_arm7
	tar --directory=dist -cz $(NAME)_arm7 > dist/$(NAME)_$(TAG)_arm7.tar.gz

	mkdir -p dist/$(NAME)_arm8
	mv $(NAME).arm8 dist/$(NAME)_arm8
	cp -fR examples dist/$(NAME)_arm8
	cp LICENSE dist/$(NAME)_arm8
	cp NOTICE dist/$(NAME)_arm8

	tar --directory=dist -cz $(NAME)_arm8 > dist/$(NAME)_$(TAG)_arm8.tar.gz

	sh -c 'cd dist; sha256sum *.tar.gz' > dist/checksums.txt
