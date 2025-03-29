# https://medium.com/@zubairidrees.cs/kafka-in-action-a-step-by-step-guide-to-building-with-spring-boot-and-docker-91411a895b5c

# https://github.com/agithomas/kafka-with-jolokia

# Lo creamos como: docker build -t domacker/kafka_with_prometheus:7.8.0 .
FROM confluentinc/cp-kafka:7.8.0

# Prometheus
EXPOSE 8080
# JMX
EXPOSE 9101

# Instalar el jar de Jolokia para para arrancar
USER root
RUN \
 mkdir -p /opt/prometheus \
 && ls -l /opt/prometheus \
 && wget https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/0.3.1/jmx_prometheus_javaagent-0.3.1.jar --no-check-certificate \
 -O /opt/prometheus/jmx_prometheus_javaagent-0.3.1.jar \
 && ls -l /opt/prometheus
 USER appuser

 COPY kafka-0-8-2.yml /opt/prometheus/
