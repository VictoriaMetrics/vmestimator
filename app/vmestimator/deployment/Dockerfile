ARG base_image=non-existing
FROM $base_image

EXPOSE 8429

ENTRYPOINT ["/vmestimator-prod"]
ARG src_binary=non-existing
COPY $src_binary ./vmestimator-prod
