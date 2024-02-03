#!/bin/bash
#Usando el contenedor de kafka. Lanzamos el script para crear el topic "telemetry" y arracamos un productor en consola para cargar los datos.
docker exec -it entorno_kafka_1 bash /opt/kafka_2.13-2.8.1/script/create_topic.sh

