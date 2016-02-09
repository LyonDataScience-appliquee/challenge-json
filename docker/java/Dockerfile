# JRE 1.8u73 with BusyBox
# Docker address for this file is :
# https://raw.githubusercontent.com/matfeb/challenge-json/master/docker/java/Dockerfile
# for the moment...

FROM progrium/busybox
MAINTAINER Mathieu Febvay <mat.febvay@hotmail.fr> thanks to Ilkka Anttonen from http://sirile.github.io/

# Get and install JRE and JAR release
RUN \
  opkg-install wget \
  && wget --no-check-certificate --no-cookies --header "Cookie: oraclelicense=accept-securebackup-cookie" -O /tmp/jre.tar.gz http://download.oracle.com/otn-pub/java/jdk/8u73-b02/jre-8u73-linux-x64.tar.gz \
  && gunzip /tmp/jre.tar.gz \
  && cd /opt && tar xf /tmp/jre.tar \
  && rm /tmp/jre.tar \
  && mkdir -p /usr/velov \
  && cd /usr/velov \
  && wget --no-check-certificate -O JSONParser.jar https://github.com/matfeb/challenge-json/releases/download/v1.2/JSONParser-1.2.jar \
  && chmod +x JSONParser.jar
  
RUN \
  ln -sf /lib/libpthread-2.18.so /lib/libpthread.so.0 \
  && ln -s /opt/jdk1.8.0_73/bin/java /usr/bin/java

