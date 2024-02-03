Este repositorio ha sido creado expresamente para el Proyecto de Fin de Master "Máster en Big Data y Data Science: ciencia e ingeniería de datos" de Pablo Machío Rueda.

Contiene todo el código necesario para implementar una arquitectura Lambda orientada al análisis de los datos de un servicio IoT que adquiere datos de telemetría de vehículos.
Spark es la herramienta que vertebra el análisis en las distintas capas de la arquitectura puesto que permite reutilizar código tanto para la capa batch como para la capa de tiempo real.
Otro motivo del uso de Spark es su librería de Machine Learning, en este caso solo buscamos probar esta librería más que evaluar los distintos modelos ofrecidos.

Todo el código de Spark está escrito en Scala.
Se han usado contenedores Docker para hacer pruebas de integración de Spark con Kafka y PostgreSQL.

El juego de datos empleado es muy pequeño porque la finalidad era probar las distintas técnologia y no su desempeño con grandes volumenes de datos.

# Estructura

- **Raiz**. Contiene los directorios principales así como los scripts para arrancar las pruebas (script\_\*.sh)y un pequeño script en python usado para limpiar los archivos json usados como fuentes de datos para las pruebas (removeNonUTFCharacter.py)
- **Carpeta Documentación**. Contiene documentación usada para crear y documentar el proyecto asi como la memoría del proyecto.
- **Carpeta codigo**. Contiene todo el código en Scala de la prueba de concepto. Incluye test unitarios. Esta preparado para Java 11, Scala 2.12 y SBT 1.8 como herramienta de construcción del proyecto
  - **Carpeta codigo/spark_proj**: Contiene el código de Spark, modelo de ML incluido
    - **Carpeta codigo/spark_proj/src/main/org.uam.masterbigdata.driver**. Contiene los drivers para las capas batch y realtime.
      - **ToSubmitDriver.scala**. Driver de capa batch
      - **StreamDriver.scala**. Driver de capa tiempo real
        **Carpeta codigo/spark_proj/src/main/org.uam.masterbigdata**. Contiene los objetos con las funciones usadas para componer las transformaciones. Existen pruebas unitarias para cada una de ellas.
        - **ClassifierHelper**. Contiene el pipeline del modelo de ML. En su prueba unitaria correspodiente está el proceso de entrenamiento.
  - **Carpeta codigo/web**: Contiene el código de la web de consulta. En Scala.

# Scripts

Todos los scripts para inicializar el entorno y realizar pruebas se encuentran en la raiz.

- **script_1_assembly_spark.sh**. Borra ensamblados previos del proyecto (evitar ejecutar una versión antigua), lanza los tests y crea finalmente el ensamblado. El ensamblado se copia en la carpeta 'entorno' para poder ejecutarlo en el entorno de pruebas (dockers).
- **script_2_inicio_entorno.sh**. Inicializa el docker compose con todo el entorno de pruebas.
  - El fichero docker-compose.yml se encuentra en la carpeta 'entorno'.
- **script_3_inicio_web.sh**. Ensambla la web y la levanta.
- **script_4_batch.sh**. Lanza en el entorno de pruebas las tareas de la capa batch.
- **script_5_stream.sh**. Lanza en el entorno de pruebas las tareas de la capa de tiempo real.
- **script_6_stream_producer.sh**. Publica en el Kafka del entorno de pruebas para que la capa de tiempo real lo pueda consumir.

Importante: El docker-compose no se ha parado en ningún momento, es responsabilidad de la persona que ejecute los scripts el pararlo.

## Ejecución

Ejecutar los scripts del 1 al 6 en orden.
El script 1 es opcional si ya se creo previamente el ensablado del proyecto Spark
El script 3 es opcional si ya se creo previamente el ensablado del proyecto Web,pero habría que ejecutar manualmente el ensamblado del proyecto (se puede ver en el script como hacerlo).

La web se puede consultar en: http://localhost:8080/api/v1.0/docs/index.html?url=/api/v1.0/docs/docs.yaml

Inicialización:
Podemos usar el sript "script_2_inicio_web.sh" o hacerlo manualmente.

Como vehículo de referencia usamos 763738942838145024

# Test unitarios

- Spark: en la carpeta codigo/spark_proj/src/test/scala
- Web: no hay es una api rest para consultar los datos de la capa de servicio.
