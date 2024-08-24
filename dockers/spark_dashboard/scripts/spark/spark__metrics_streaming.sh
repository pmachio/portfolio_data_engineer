#!/bin/bash
#Arranca una una shell spark en scala con la configuración de las metricas para enviar a contenedor spark-dashboar

#destino de las metricas
export VICTORIAMETRICS_ENDPOINT='spark-dashboard'

#Inicializa el shell con la configuración básica 
./spark/bin/spark-shell --conf "spark.metrics.conf.*.sink.graphite.class"="org.apache.spark.metrics.sink.GraphiteSink" \
 --conf "spark.metrics.conf.*.sink.graphite.host"=$VICTORIAMETRICS_ENDPOINT \
 --conf "spark.metrics.conf.*.sink.graphite.port"=2003 \
 --conf "spark.metrics.conf.*.sink.graphite.period"=10 \
 --conf "spark.metrics.conf.*.sink.graphite.unit"=seconds \
 --conf "spark.metrics.conf.*.sink.graphite.prefix"="lucatest" \
 --conf "spark.metrics.conf.*.source.jvm.class"="org.apache.spark.metrics.source.JvmSource" \
 --conf "spark.metrics.staticSources.enabled"=true \
 --conf "spark.metrics.appStatusSource.enabled"=true \
 --conf "spark.sql.streaming.metricsEnabled"=true \
 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2