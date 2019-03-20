FROM gradle:4.10-jdk8-alpine as builder
MAINTAINER publicplan GmbH
LABEL MAINTAINER="publicplan GmbH"

COPY --chown=gradle:gradle ./ /app/yacy_grid_loader
WORKDIR /app/yacy_grid_loader

RUN gradle shadowJar && \
    mkdir data/loader-8200/conf/ -p && \
    cp -r conf/* data/loader-8200/conf/ && \
    cp docker/config-loader.properties data/loader-8200/conf/config.properties

FROM openjdk:8-alpine as runner

WORKDIR /app

COPY --from=builder /app/yacy_grid_loader/build/libs/*.jar /app/yacy-grid-loader.jar
COPY --from=builder /app/yacy_grid_loader/data /app/data

EXPOSE 8100
ENTRYPOINT ["java"]
CMD ["-jar", "yacy-grid-loader.jar"]
