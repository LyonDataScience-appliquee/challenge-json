# JDK 1.8u73 with BusyBox

FROM progrium/busybox
MAINTAINER Mathieu Febvay <mat.febvay@hotmail.fr> thank to Ilkka Anttonen from http://sirile.github.io/

COPY . /usr/velov
WORKDIR /usr/velov

# Udate wget to support SSL
RUN opkg-install wget

# Get and install Java
RUN \
  wget --no-check-certificate --no-cookies --header "Cookie: oraclelicense=accept-securebackup-cookie" -O /tmp/jdk.tar.gz http://download.oracle.com/otn-pub/java/jdk/8u73-b02/jdk-8u73-linux-x64.tar.gz && \
  gunzip /tmp/jdk.tar.gz && \
  cd /opt && tar xf /tmp/jdk.tar && \
  rm /tmp/jdk.tar && \

# Link Java into use, wget-ssl updates libpthread which causes Java to break
RUN \
  
