PKG_PREFIX := github.com/VictoriaMetrics/vmestimator

MAKE_CONCURRENCY ?= $(shell getconf _NPROCESSORS_ONLN)
MAKE_PARALLEL := $(MAKE) -j $(MAKE_CONCURRENCY)
DATEINFO_TAG ?= $(shell date -u +'%Y%m%d-%H%M%S')
BUILDINFO_TAG ?= $(shell echo $$(git describe --long --all | tr '/' '-')$$( \
	      git diff-index --quiet HEAD -- || echo '-dirty-'$$(git diff-index -u HEAD | openssl sha1 | cut -d' ' -f2 | cut -c 1-8)))

PKG_TAG ?= $(shell git tag -l --points-at HEAD)
ifeq ($(PKG_TAG),)
PKG_TAG := $(BUILDINFO_TAG)
endif

EXTRA_DOCKER_TAG_SUFFIX ?=
EXTRA_GO_BUILD_TAGS ?=

GO_BUILDINFO = -X 'github.com/VictoriaMetrics/VictoriaMetrics/lib/buildinfo.Version=c=$(APP_NAME)-$(DATEINFO_TAG)-$(BUILDINFO_TAG)'
TAR_OWNERSHIP ?= --owner=1000 --group=1000

GOLANGCI_LINT_VERSION := 2.12.2

.PHONY: $(MAKECMDGOALS)

include app/*/Makefile
#include codespell/Makefile
#include docs/Makefile
include deployment/*/Makefile
#include dashboards/Makefile
#include package/release/Makefile
#include benchmarks/Makefile

all: \
	vmestimator-prod

clean:
	rm -rf bin/*

publish: \
	publish-vmestimator

package: \
	package-vmestimator

# When adding a new crossbuild target, please also add it to the .github/workflows/build.yml
crossbuild:
	$(MAKE_PARALLEL) vmestimator-crossbuild

# When adding a new crossbuild target, please also add it to the .github/workflows/build.yml
vmestimator-crossbuild: \
	vmestimator-linux-386 \
	vmestimator-linux-amd64 \
	vmestimator-linux-arm64 \
	vmestimator-linux-arm \
	vmestimator-linux-ppc64le \
	vmestimator-darwin-amd64 \
	vmestimator-darwin-arm64 \
	vmestimator-freebsd-amd64 \
	vmestimator-openbsd-amd64 \
	vmestimator-windows-amd64

publish-final-images:
	PKG_TAG=$(TAG) APP_NAME=vmestimator $(MAKE) publish-via-docker-from-rc && \
	PKG_TAG=$(TAG) $(MAKE) publish-latest

publish-latest:
	PKG_TAG=$(TAG) APP_NAME=vmestimator $(MAKE) publish-via-docker-latest

release:
	$(MAKE_PARALLEL) \
		release-vmestimator

release-vmestimator: \
	release-vmestimator-linux-386 \
	release-vmestimator-linux-amd64 \
	release-vmestimator-linux-arm \
	release-vmestimator-linux-arm64 \
	release-vmestimator-linux-s390x \
	release-vmestimator-darwin-amd64 \
	release-vmestimator-darwin-arm64 \
	release-vmestimator-freebsd-amd64 \
	release-vmestimator-openbsd-amd64 \
	release-vmestimator-windows-amd64

release-vmestimator-linux-386:
	GOOS=linux GOARCH=386 $(MAKE) release-vmestimator-goos-goarch

release-vmestimator-linux-amd64:
	GOOS=linux GOARCH=amd64 $(MAKE) release-vmestimator-goos-goarch

release-vmestimator-linux-arm:
	GOOS=linux GOARCH=arm $(MAKE) release-vmestimator-goos-goarch

release-vmestimator-linux-arm64:
	GOOS=linux GOARCH=arm64 $(MAKE) release-vmestimator-goos-goarch

release-vmestimator-linux-s390x:
	GOOS=linux GOARCH=s390x $(MAKE) release-vmestimator-goos-goarch

release-vmestimator-darwin-amd64:
	GOOS=darwin GOARCH=amd64 $(MAKE) release-vmestimator-goos-goarch

release-vmestimator-darwin-arm64:
	GOOS=darwin GOARCH=arm64 $(MAKE) release-vmestimator-goos-goarch

release-vmestimator-freebsd-amd64:
	GOOS=freebsd GOARCH=amd64 $(MAKE) release-vmestimator-goos-goarch

release-vmestimator-openbsd-amd64:
	GOOS=openbsd GOARCH=amd64 $(MAKE) release-vmestimator-goos-goarch

release-vmestimator-windows-amd64:
	GOARCH=amd64 $(MAKE) release-vmestimator-windows-goarch

release-vmestimator-goos-goarch: vmestimator-$(GOOS)-$(GOARCH)-prod
	cd bin && \
		tar $(TAR_OWNERSHIP) --transform="flags=r;s|-$(GOOS)-$(GOARCH)||" -czf vmestimator-$(GOOS)-$(GOARCH)-$(PKG_TAG).tar.gz \
			vmestimator-$(GOOS)-$(GOARCH)-prod \
		&& sha256sum vmestimator-$(GOOS)-$(GOARCH)-$(PKG_TAG).tar.gz \
			vmestimator-$(GOOS)-$(GOARCH)-prod \
			| sed s/-$(GOOS)-$(GOARCH)-prod/-prod/ > vmestimator-$(GOOS)-$(GOARCH)-$(PKG_TAG)_checksums.txt
	cd bin && rm -rf vmestimator-$(GOOS)-$(GOARCH)-prod

release-vmestimator-windows-goarch: vmestimator-windows-$(GOARCH)-prod
	cd bin && \
		zip vmestimator-windows-$(GOARCH)-$(PKG_TAG).zip \
			vmestimator-windows-$(GOARCH)-prod.exe \
		&& sha256sum vmestimator-windows-$(GOARCH)-$(PKG_TAG).zip \
			vmestimator-windows-$(GOARCH)-prod.exe \
			> vmestimator-windows-$(GOARCH)-$(PKG_TAG)_checksums.txt
	cd bin && rm -rf \
		vmestimator-windows-$(GOARCH)-prod.exe


pprof-cpu:
	go tool pprof -trim_path=github.com/VictoriaMetrics/VictoriaMetrics $(PPROF_FILE)

fmt:
	gofmt -l -w -s ./app

vet:
	go vet ./app/...

check-all: fmt vet golangci-lint govulncheck

clean-checkers: remove-golangci-lint remove-govulncheck

test:
	go test -tags 'synctest' ./app/...

test-race:
	go test -tags 'synctest' -race ./app/...

test-386:
	GOARCH=386 go test -tags 'synctest' ./app/...

test-pure:
	CGO_ENABLED=0 go test -tags 'synctest' ./app/...

test-full:
	go test -tags 'synctest' -coverprofile=coverage.txt -covermode=atomic ./app/...

test-full-386:
	GOARCH=386 go test -tags 'synctest' -coverprofile=coverage.txt -covermode=atomic ./app/...

vendor-update:
	go get -u ./app/...
	go mod tidy -compat=1.26
	go mod vendor

app-local:
	CGO_ENABLED=1 go build $(RACE) -ldflags "$(GO_BUILDINFO)" -tags "$(EXTRA_GO_BUILD_TAGS)" -o bin/$(APP_NAME)$(RACE) $(PKG_PREFIX)/app/$(APP_NAME)

app-local-pure:
	CGO_ENABLED=0 go build $(RACE) -ldflags "$(GO_BUILDINFO)" -tags "$(EXTRA_GO_BUILD_TAGS)" -o bin/$(APP_NAME)-pure$(RACE) $(PKG_PREFIX)/app/$(APP_NAME)

app-local-goos-goarch:
	CGO_ENABLED=$(CGO_ENABLED) GOOS=$(GOOS) GOARCH=$(GOARCH) go build $(RACE) -ldflags "$(GO_BUILDINFO)" -tags "$(EXTRA_GO_BUILD_TAGS)" -o bin/$(APP_NAME)-$(GOOS)-$(GOARCH)$(RACE) $(PKG_PREFIX)/app/$(APP_NAME)

app-local-windows-goarch:
	CGO_ENABLED=0 GOOS=windows GOARCH=$(GOARCH) go build $(RACE) -ldflags "$(GO_BUILDINFO)" -tags "$(EXTRA_GO_BUILD_TAGS)" -o bin/$(APP_NAME)-windows-$(GOARCH)$(RACE).exe $(PKG_PREFIX)/app/$(APP_NAME)

golangci-lint: install-golangci-lint
	golangci-lint run --build-tags 'synctest'

install-golangci-lint:
	which golangci-lint && (golangci-lint --version | grep -q $(GOLANGCI_LINT_VERSION)) || curl -sSfL https://golangci-lint.run/install.sh | sh -s -- -b $(shell go env GOPATH)/bin v$(GOLANGCI_LINT_VERSION)

remove-golangci-lint:
	rm -rf `which golangci-lint`

govulncheck: install-govulncheck
	govulncheck ./...

govulncheck-docker:
	docker run -w $(PWD) -v $(PWD):$(PWD) \
		-v govulncheck-gomod-cache:/root/go/pkg/mod \
		-v govulncheck-gobuild-cache:/root/.cache/go-build \
		-v govulncheck-go-bin:/root/go/bin \
		--env="GOCACHE=/root/.cache/go-build" \
		--env="GOMODCACHE=/root/go/pkg/mod" \
		"$(GO_BUILDER_IMAGE)" /bin/sh -c "which govulncheck || go install golang.org/x/vuln/cmd/govulncheck@latest && govulncheck ./..."

install-govulncheck:
	which govulncheck || go install golang.org/x/vuln/cmd/govulncheck@latest

remove-govulncheck:
	rm -rf `which govulncheck`

install-wwhrd:
	which wwhrd || go install github.com/frapposelli/wwhrd@latest

check-licenses: install-wwhrd
	wwhrd check -f .wwhrd.yml
