# All these commands must run from repository root.

vmestimator:
	APP_NAME=vmestimator $(MAKE) app-local

vmestimator-race:
	APP_NAME=vmestimator RACE=-race $(MAKE) app-local

vmestimator-prod:
	APP_NAME=vmestimator $(MAKE) app-via-docker

vmestimator-pure-prod:
	APP_NAME=vmestimator $(MAKE) app-via-docker-pure

vmestimator-linux-amd64-prod:
	APP_NAME=vmestimator $(MAKE) app-via-docker-linux-amd64

vmestimator-linux-arm-prod:
	APP_NAME=vmestimator $(MAKE) app-via-docker-linux-arm

vmestimator-linux-arm64-prod:
	APP_NAME=vmestimator $(MAKE) app-via-docker-linux-arm64

vmestimator-linux-ppc64le-prod:
	APP_NAME=vmestimator $(MAKE) app-via-docker-linux-ppc64le

vmestimator-linux-386-prod:
	APP_NAME=vmestimator $(MAKE) app-via-docker-linux-386

vmestimator-linux-s390x-prod:
	APP_NAME=vmestimator $(MAKE) app-via-docker-linux-s390x

vmestimator-darwin-amd64-prod:
	APP_NAME=vmestimator $(MAKE) app-via-docker-darwin-amd64

vmestimator-darwin-arm64-prod:
	APP_NAME=vmestimator $(MAKE) app-via-docker-darwin-arm64

vmestimator-freebsd-amd64-prod:
	APP_NAME=vmestimator $(MAKE) app-via-docker-freebsd-amd64

vmestimator-openbsd-amd64-prod:
	APP_NAME=vmestimator $(MAKE) app-via-docker-openbsd-amd64

vmestimator-windows-amd64-prod:
	APP_NAME=vmestimator $(MAKE) app-via-docker-windows-amd64

package-vmestimator:
	APP_NAME=vmestimator $(MAKE) package-via-docker

package-vmestimator-pure:
	APP_NAME=vmestimator $(MAKE) package-via-docker-pure

package-vmestimator-amd64:
	APP_NAME=vmestimator $(MAKE) package-via-docker-amd64

package-vmestimator-arm:
	APP_NAME=vmestimator $(MAKE) package-via-docker-arm

package-vmestimator-arm64:
	APP_NAME=vmestimator $(MAKE) package-via-docker-arm64

package-vmestimator-ppc64le:
	APP_NAME=vmestimator $(MAKE) package-via-docker-ppc64le

package-vmestimator-386:
	APP_NAME=vmestimator $(MAKE) package-via-docker-386

publish-vmestimator:
	APP_NAME=vmestimator $(MAKE) publish-via-docker

run-vmestimator:
	APP_NAME=vmestimator $(MAKE) run-via-docker

vmestimator-linux-amd64:
	APP_NAME=vmestimator CGO_ENABLED=1 GOOS=linux GOARCH=amd64 $(MAKE) app-local-goos-goarch

vmestimator-linux-arm:
	APP_NAME=vmestimator CGO_ENABLED=0 GOOS=linux GOARCH=arm $(MAKE) app-local-goos-goarch

vmestimator-linux-arm64:
	APP_NAME=vmestimator CGO_ENABLED=0 GOOS=linux GOARCH=arm64 $(MAKE) app-local-goos-goarch

vmestimator-linux-ppc64le:
	APP_NAME=vmestimator CGO_ENABLED=0 GOOS=linux GOARCH=ppc64le $(MAKE) app-local-goos-goarch

vmestimator-linux-s390x:
	APP_NAME=vmestimator CGO_ENABLED=0 GOOS=linux GOARCH=s390x $(MAKE) app-local-goos-goarch

vmestimator-linux-loong64:
	APP_NAME=vmestimator CGO_ENABLED=0 GOOS=linux GOARCH=loong64 $(MAKE) app-local-goos-goarch

vmestimator-linux-386:
	APP_NAME=vmestimator CGO_ENABLED=0 GOOS=linux GOARCH=386 $(MAKE) app-local-goos-goarch

vmestimator-darwin-amd64:
	APP_NAME=vmestimator CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 $(MAKE) app-local-goos-goarch

vmestimator-darwin-arm64:
	APP_NAME=vmestimator CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 $(MAKE) app-local-goos-goarch

vmestimator-freebsd-amd64:
	APP_NAME=vmestimator CGO_ENABLED=0 GOOS=freebsd GOARCH=amd64 $(MAKE) app-local-goos-goarch

vmestimator-openbsd-amd64:
	APP_NAME=vmestimator CGO_ENABLED=0 GOOS=openbsd GOARCH=amd64 $(MAKE) app-local-goos-goarch

vmestimator-windows-amd64:
	GOARCH=amd64 APP_NAME=vmestimator $(MAKE) app-local-windows-goarch

vmestimator-pure:
	APP_NAME=vmestimator $(MAKE) app-local-pure

### Packaging as DEB - amd64
vmestimator-package-deb-amd64: vmestimator-linux-amd64-prod
	./package/package_deb.sh amd64

### Packaging as DEB - arm64
vmestimator-package-deb-arm: vmestimator-linux-arm-prod
	./package/package_deb.sh arm

### Packaging as DEB - arm64
vmestimator-package-deb-arm64: vmestimator-linux-arm64-prod
	./package/package_deb.sh arm64

### Packaging as DEB - all
vmestimator-package-deb: \
        vmestimator-package-deb-amd64 \
        vmestimator-package-deb-arm \
        vmestimator-package-deb-arm64

### Packaging as RPM - amd64
vmestimator-package-rpm-amd64: vmestimator-linux-amd64-prod
	./package/package_rpm.sh amd64

### Packaging as RPM - arm64
vmestimator-package-rpm-arm64: vmestimator-linux-arm64-prod
	./package/package_rpm.sh arm64

### Packaging as RPM - all
vmestimator-package-rpm: \
        vmestimator-package-rpm-amd64 \
        vmestimator-package-rpm-arm64

### Packaging as both DEB and RPM - all
vmestimator-package-deb-rpm: \
        vmestimator-package-deb \
        vmestimator-package-rpm
