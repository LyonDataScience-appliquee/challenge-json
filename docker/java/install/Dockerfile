# Install official Oracle JRE 8 on Ubuntu
#
# Original Dockerfile from dockerfile/java with changes
# Installation is OK
# STAND BY FOR THIS ONE
#
# Oracle Java 8 Dockerfile
#
#

# Pull base image.
FROM ubuntu

MAINTAINER Mathieu Febvay <mat.febvay@hotmail.fr>

# Install Java.
RUN \
  echo oracle-java8-installer shared/accepted-oracle-license-v1-1 select true | debconf-set-selections && \
  apt-get install -y software-properties-common && \
  add-apt-repository -y ppa:webupd8team/java && \
  apt-get update && \
  apt-get install -y oracle-java8-installer && \
  rm -rf /var/lib/apt/lists/* && \
  rm -rf /var/cache/oracle-jdk8-installer


# Define working directory.
WORKDIR /data

# Define commonly used JAVA_HOME variable
ENV JAVA_HOME /usr/lib/jvm/java-8-oracle

# Define default command.
#CMD ["bash"]
