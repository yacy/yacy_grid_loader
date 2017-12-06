FROM ubuntu:latest
MAINTAINER publicplan GmbH

# update
RUN apt-get update
RUN apt-get upgrade -y

# install debug tools
RUN apt-get install -y curl
RUN apt-get install -y dnsutils inetutils-tools inetutils-ping

# add packages
RUN apt-get install -y git openjdk-8-jdk

# install gradle required for build
RUN apt-get update && apt-get install -y software-properties-common
RUN add-apt-repository ppa:cwchien/gradle
RUN apt-get update
RUN apt-get install -y wget
RUN wget https://services.gradle.org/distributions/gradle-3.4.1-bin.zip
RUN mkdir /opt/gradle
RUN apt-get install -y unzip
RUN unzip -d /opt/gradle gradle-3.4.1-bin.zip
RUN PATH=$PATH:/opt/gradle/gradle-3.4.1/bin
ENV GRADLE_HOME=/opt/gradle/gradle-3.4.1
ENV PATH=$PATH:$GRADLE_HOME/bin
RUN gradle -v

# change workdir
ADD ./ /yacy_grid_loader
WORKDIR /yacy_grid_loader

# compile
RUN gradle build
RUN mkdir data/loader-8200/conf/ -p
RUN cp docker/config-loader.properties data/loader-8200/conf/config.properties
RUN chmod +x ./docker/start.sh

# expose web interface port
EXPOSE 8200

# define default command.
ENTRYPOINT ["/bin/bash", "./docker/start.sh"]
