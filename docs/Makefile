UID := "$(shell id -u)"
PLATFORM := $(shell uname -m)
DOCKER_PLATFORM := "linux/$(if $(findstring $(PLATFORM),arm64),arm64,amd64)"


# These commands must be run from the VictoriaMetrics repository root

docs-image:
	if [ ! -d vmdocs ]; then \
		git clone --depth 1 git@github.com:VictoriaMetrics/vmdocs vmdocs; \
	fi; \
	cd vmdocs && \
	git checkout main && \
	git pull origin main && \
	cd .. && \
	docker build \
		-t vmdocs-docker-package \
		--build-arg UID=$(UID) \
		--platform $(DOCKER_PLATFORM) \
		vmdocs

docs-check-links: docs-image
	rm -rf vmdocs/public
	docker run \
		--rm \
		--platform $(DOCKER_PLATFORM) \
		-v ./vmdocs:/opt/docs \
		$(shell for d in ./docs/*/; do printf ' -v %s:/opt/docs/content/%s' "$${d}" "$$(basename $${d})"; done) \
		--entrypoint /bin/sh \
		vmdocs-docker-package \
			-c "yarn install && hugo --minify && yarn run check-links"

docs-debug: docs docs-image
	docker run \
		--rm \
		--name vmdocs-docker-container \
		--platform $(DOCKER_PLATFORM) \
		-p 1313:1313 \
		$(foreach dir,$(wildcard ./docs/$(dir)/*), -v ./docs/$(notdir $(dir)):/opt/docs/content/$(notdir $(dir))) \
			vmdocs-docker-package

# Converts images at docs folder to webp format
# See https://docs.victoriametrics.com/victoriametrics/single-server-victoriametrics/#images-in-documentation
docs-images-to-webp: docs-image
	docker run \
		--rm \
		--platform $(DOCKER_PLATFORM) \
		--entrypoint /usr/bin/find \
		--name vmdocs-docker-container \
		-v ./docs:/opt/docs/content/victoriametrics vmdocs-docker-package \
			content \
				-regex ".*\.\(png\|jpg\|jpeg\)" \
				-exec sh -c 'cwebp -preset drawing -m 6 -o $$(echo {} | cut -f-1 -d.).webp {} && rm -rf {}' {} \;
