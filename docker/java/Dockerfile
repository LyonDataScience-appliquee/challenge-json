# JDK 1.8u73 with BusyBox

FROM progrium/busybox
MAINTAINER Mathieu Febvay <mat.febvay@hotmail.fr> thanks to Ilkka Anttonen from http://sirile.github.io/

ENV depot = matfeb

# Get and install Java and Git
RUN \
  opkg-install wget git \
  && wget -r https://github.com/matfeb/challenge-json/blob/master/docker/java/script -O /usr/velov/ \
  && wget --no-check-certificate --no-cookies --header "Cookie: oraclelicense=accept-securebackup-cookie" -O /tmp/jdk.tar.gz http://download.oracle.com/otn-pub/java/jdk/8u73-b02/jdk-8u73-linux-x64.tar.gz \
  && gunzip /tmp/jdk.tar.gz \
  && cd /opt && tar xf /tmp/jdk.tar \
  && rm /tmp/jdk.tar \
  && ln -sf /lib/libpthread-2.18.so /lib/libpthread.so.0 \
  && ln -s /opt/jdk1.8.0_73/bin/java /usr/bin/java \
  && ln -s /opt/jdk1.8.0_73/bin/javac /usr/bin/javac \
  && mkdir -p /usr/velov \
  && cd /usr/velov \
  && git clone git://github.com/matfeb/challenge-json/master/docker/java/script.git script
  #chmod + args.sh && \
  #javac JSONParser.java
