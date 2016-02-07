# JRE 1.8u73 with BusyBox

FROM progrium/busybox
MAINTAINER Mathieu Febvay <mat.febvay@hotmail.fr> thank to Ilkka Anttonen from http://sirile.github.io/

# Udate wget to support SSL
RUN opkg-install wget

# Get and install Java
RUN \
  wget --no-check-certificate --no-cookies --header "Cookie: oraclelicense=accept-securebackup-cookie" -O /tmp/jre.tar.gz http://download.oracle.com/otn-pub/java/jdk/8u73-b02/jre-8u73-linux-x64.tar.gz && \
  gunzip /tmp/jre.tar.gz && \
  cd /opt && tar xf /tmp/jre.tar && \
  rm /tmp/jre.tar

# Link Java into use, wget-ssl updates libpthread which causes Java to break
RUN \
  ln -sf /lib/libpthread-2.18.so /lib/libpthread.so.0 && \
  ln -s /opt/jre1.8.0_73/bin/java /usr/bin/java && \
  ln -s /opt/jre1.8.0_73/bin/javac /usr/bin/javac
