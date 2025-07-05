# Modificación sobre el original de lucacanli

Modificamos la configuracioón de influxdb para añadir a Telegraf el pluguin HTTP Input para que recupere del HBase el dump con las metricas que está en formato json

## How to run

Run the dashboard using a container image from [Dockerhub](https://hub.docker.com/r/lucacanali/spark-dashboard):

- There are a few ports needed and multiple options on how to expose them
- Port 2003 is for Graphite ingestion, port 3000 is for Grafana, port 8086 is used internally by the Grafana source
- You can expose the ports from the container individually or just make `network=host`.
- Examples:

```
docker run --network=host -d ldomacker/spark-dashboard_hbase
or
docker run -p 3000:3000 -p 2003:2003 -d domacker/spark-dashboard_hbase
or
docker run -p 3000:3000 -p 2003:2003 -p 8086:8086 -d domacker/spark-dashboard_hbase
```

## Advanced: persist InfluxDB data across restarts

- This shows an example of how to use a volume to store InfluxDB data.
  It allows preserving the history across runs when the container is restarted,
  otherwise InfluxDB starts from scratch each time.

```
docker run --network=host -v MYPATH/myinfluxdir:/var/lib/influxdb -d domacker/spark-dashboard_hbase
```

## How to build the image:

```
docker build -t spark-dashboard:v01 .
```
