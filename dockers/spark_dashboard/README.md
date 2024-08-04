# Introducción

Ejemplo de aplicación de SparkDashboard basado en la [implementación de Luca Canali](https://github.com/LucaCanali/Miscellaneous/tree/master/Spark_Dashboard)

Creamos un docker compose con un contenedor de Spark y otro con el dashboard de Spark, de forma que podemos usar la consola de Spark para alimentar el dashboard.
También podemos usar el contenedor de Spark para subir alguna aplicacion del tipo que queramos y probarla.

## Tareas pendietes.

1. Pasar establecer un volumen para el contenedor de spark para poder pasar, aplicaciones, properties...
2. Establecer la variable de entorno para pasar la configuración de las metricas en un properties con el que podamos jugar.

## Como pasar las configuraciones

### Prueba básica 1, desde la shell pasando los parámetros directamente

1. Levantar el compose.
2. Entrar en el master.
3. Definir la variable de entorno para en la que queremos enviar las metricas

```SHELL
export VICTORIAMETRICS_ENDPOINT='spark-dashboard'
```

4. Desde el directorio bin de spark lanzar la shell con los párametros de configuracion

```SHELL
./spark-shell --conf "spark.metrics.conf.*.sink.graphite.class"="org.apache.spark.metrics.sink.GraphiteSink" \
 #Configuracion del sink
 --conf "spark.metrics.conf.*.sink.graphite.host"=$VICTORIAMETRICS_ENDPOINT \
 --conf "spark.metrics.conf.*.sink.graphite.port"=2003 \
 --conf "spark.metrics.conf.*.sink.graphite.period"=10 \
 --conf "spark.metrics.conf.*.sink.graphite.unit"=seconds \
 --conf "spark.metrics.conf.*.sink.graphite.prefix"="lucatest" \
 #Configuracion metricas
 --conf "spark.metrics.conf.*.source.jvm.class"="org.apache.spark.metrics.source.JvmSource" \
 #Metricas que queremos recorger
 --conf "spark.metrics.staticSources.enabled"=true \
 --conf "spark.metrics.appStatusSource.enabled"=true
```

### Prueba básica 1.1, desde la shell pasando los parámetros directamente para una aplicación de stream

## Pruebas de metricas

### Común

**_Pendiente_**

### Aplicacion batch

**_Pendiente_**

### Aplicacion batch

**_Streaming_**
