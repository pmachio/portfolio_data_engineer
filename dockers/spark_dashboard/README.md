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

```SHELL del host
docker compose -f docker-compose.yml up
```

2. Entrar en el master y en incializar la shell de spark usando el script que se ha copiado en el volumen

```SHELL del host
docker exec -it $(docker-compose -f docker-compose_streaming.yml ps -q spark-master) bash
```

```SHELL del contenedor
sh /opt/spark_shell_scripts/spark__metrics_streaming.sh
```

3. y ya en la consola, con la sesion de spark inicializada, podemos lanzar un job para ver como el dashboard se actualiza

```SHELL Consola de spark
val rdd = sc.parallelize(0 to 10000000)
rdd.count()
```

### Prueba básica 1.1, desde la shell pasando los parámetros directamente para una aplicación de stream

1. Levantar el compose.

```SHELL del host
docker compose -f docker-compose_streaming.yml up
```

2. Entrar en el contenedor de kafka e iniciar el productor ejecutan el script que se copia en el volumen

```SHELL del host
docker exec -it $(docker-compose -f docker-compose_streaming.yml ps -q kafka) bash
```

```SHELL del contenedor de kafka
sh /opt/kafka/script/kafka_producer.sh
```

2. Entrar en el master de spark e iniciar la shell con la configuración para las metricas de streaming usando el script que se copia en el volumen

```SHELL del host
docker exec -it $(docker-compose -f docker-compose_streaming.yml ps -q spark-master) bash
```

```SHELL del contenedor
sh /opt/spark_shell_scripts/spark__metrics_streaming.sh
```

3.  Y ya en la consola, con la sesion de spark inicializada, podemos lanzar un job para ver como el dashboard se modifica

```SHELL
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
val kafkaDF = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "kafka:9092").option("subscribe", "test").load()

kafkaDF.select(col("topic"), expr("cast(value as string) as actualValue")).writeStream.format("console").outputMode("append").start().awaitTermination(60000)
```

## Pruebas de metricas

### Común

**_Pendiente_**

### Aplicacion batch

**_Pendiente_**

### Aplicacion batch

**_Streaming_**
