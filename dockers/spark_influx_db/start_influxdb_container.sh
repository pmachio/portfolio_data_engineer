#!/bin/bash
#incia contenedor de influx db
#Sin volumen, los datos se borran entre sesiones
docker run --rm --name influxdb_metrics -e DOCKER_INFLUXDB_INIT_MODE=setup -e DOCKER_INFLUXDB_INIT_USERNAME=user_metrics -e DOCKER_INFLUXDB_INIT_PASSWORD=Metrics12 -e DOCKER_INFLUXDB_INIT_ORG=metrics -e DOCKER_INFLUXDB_INIT_BUCKET=metrics -p 8086:8086 influxdb:2.0.7 