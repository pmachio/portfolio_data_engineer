# https://medium.com/@zubairidrees.cs/kafka-in-action-a-step-by-step-guide-to-building-with-spring-boot-and-docker-91411a895b5c

# https://github.com/agithomas/kafka-with-jolokia

# Lo creamos como docker build -t domacker/kafka_with_jolokia:2.8.1 . s
FROM domacker/kafka:2.8.1

# Jolokia

EXPOSE 8778

# JMX

EXPOSE 9101

# Instalar el jar de Jolokia para para arrancar

ARG JOLOKIA_VERSION=1.7.2
ARG JOLOKIA_VERSION=${JOLOKIA_VERSION}  
ARG JOLOKIA_SOURCE_JAR=jolokia-jvm-${JOLOKIA_VERSION}.jar
ARG JOLOKIA_TARGET_JAR=jolokia-jvm.jar

RUN \
 mkdir -p /opt/jolokia \
 && ls -l /opt/jolokia \
 && wget http://search.maven.org/remotecontent?filepath=org/jolokia/jolokia-jvm/${JOLOKIA_VERSION}/${JOLOKIA_SOURCE_JAR} \
 --no-check-certificate \
 -O /opt/jolokia/$JOLOKIA_TARGET_JAR \
 && ls -l /opt/jolokia
