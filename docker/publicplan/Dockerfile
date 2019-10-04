FROM gradle:5.4-jdk8-alpine as builder
MAINTAINER publicplan GmbH
LABEL MAINTAINER="publicplan GmbH"

COPY --chown=gradle:gradle ./ /app/yacy_grid_loader
WORKDIR /app/yacy_grid_loader

RUN gradle build --info && \
    mkdir build/distributions/app && \
    unzip build/distributions/*.zip -d build/distributions/app && \
    mkdir data/loader-8200/conf/ -p && \
    cp -rf conf/* data/loader-8200/conf/

FROM openjdk:8-alpine as runner

COPY --from=builder /app/yacy_grid_loader/build/distributions/app/* /app
COPY --from=builder /app/yacy_grid_loader/data /app/data

WORKDIR /app
EXPOSE 8200
ENTRYPOINT ["/bin/sh"]
CMD ["bin/yacy_grid_loader"]
