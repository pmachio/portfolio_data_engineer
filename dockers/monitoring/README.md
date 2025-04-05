# Introduccion

Idea: montar una monitorizacion completa para
Kafka, HBase, tal vez kudu y Spark.

Pendiente:

- Añadir Grafana y Prometheus. Terminar ver (curos)[https://www.udemy.com/course/kafka-monitoring-and-operations/learn/lecture/11334308#overview]
- Spark: Crear 2 aplicaciones totas, una que lee de consola y publica en Kafka y otra que lee de Kafka y publica en Hbase
- Hbase: Añadir el contenedor y probar que recogemos las metricas

Actualemnte solo funiona con Promethius, queda la version de Jolokia + Telegraph

Interesante

- (Ejemplo Prometheus + grafana)[https://medium.com/@oredata-engineering/setting-up-prometheus-grafana-for-kafka-on-docker-8a692a45966c]

## Cluster Kafka

Basado en [articulo](https://medium.com/@darshak.kachchhi/setting-up-a-kafka-cluster-using-docker-compose-a-step-by-step-guide-a1ee5972b122).

Se han creado 2 docker build, una para usar Prometheus y la otra para Jolokia + Telgraph e Influx

## Promethious

Pendiente de añadir Grafana

## Jolika

Todo pendiente, que funcione con Jolokia e Influx + Telegraph junto con Grafana
