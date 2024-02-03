#!/bin/bash
#Usando el contenedor de kafka. Lanzamos el script para crear el topic "telemetry" y arracamos un productor en consola para cargar los datos.
#docker exec -it entorno_kafka_1 bash /opt/kafka_2.13-2.8.1/script/create_topic.sh

#Enviamos al cluster el driver de spark stream
#Notas que pasamos como paquete spark-sql-kafka que debe concindir co las version de spark que tiene el contenedor
docker exec -it entorno_spark-worker_1 /spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 --class org.uam.masterbigdata.drivers.StreamDriver --deploy-mode client --master spark://spark-master:7077 --verbose --supervise /opt/spark-apps/spark_project.jar
