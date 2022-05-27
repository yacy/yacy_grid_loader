## yacy_grid_loader dockerfile
## examples:
# docker build -t yacy_grid_loader .
# docker run -d --rm -p 8200:8200 --name yacy_grid_loader yacy_grid_loader
## Check if the service is running:
# curl http://localhost:8200/yacy/grid/mcp/info/status.json

# build app
FROM eclipse-temurin:8-jdk-alpine AS appbuilder
COPY ./ /app
WORKDIR /app
RUN ./gradlew clean shadowDistTar

# build dist
FROM eclipse-temurin:8-jre-alpine
LABEL maintainer="Michael Peter Christen <mc@yacy.net>"
ENV DEBIAN_FRONTEND noninteractive
ARG default_branch=master
COPY ./conf /app/conf/
COPY --from=appbuilder /app/build/libs/ ./app/build/libs/
WORKDIR /app
EXPOSE 8200

# for some weird reason the jar file is sometimes not named correctly
RUN if [ -e /app/build/libs/app-0.0.1-SNAPSHOT-all.jar ] ; then mv /app/build/libs/app-0.0.1-SNAPSHOT-all.jar /app/build/libs/yacy_grid_loader-0.0.1-SNAPSHOT-all.jar; fi

CMD ["java", "-jar", "/app/build/libs/yacy_grid_loader-0.0.1-SNAPSHOT-all.jar"]
