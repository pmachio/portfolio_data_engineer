Este repositorio ha sido creado expresamente para el Proyecto de Fin de Master "Máster en Big Data y Data Science: ciencia e ingeniería de datos" de Pablo Machío Rueda.

# Estructura

- **Raiz**. Contiene los directorios principale, los scripts para arrancar las pruebas
  y un pequeño script en python usado para limpiar los archivos json usados como fuentes de datos para las pruebas
- **Carpeta Documentación**. Contiene documentación usada para crear y documentar el proyecto asi como la memoría del proyecto.
- **Carpeta codigo**. Contiene todo el código en Scala de la prueba de concepto. Incluye test unitarios. Esta preparado para Java 11, Scala 2.12 y SBT 1.8 como herramienta de construcción del proyecto
  - **Carpeta codigo/spark_proj**: Contiene el código de Spark, modelo de ML incluido
    - **Carpeta codigo/spark_proj/src/main/org.uam.masterbigdata.driver**. Contiene los drivers para las capas batch y realtime.
      - **ToSubmitDriver.scala**. Driver de capa batch
      - **StreamDriver.scala**. Driver de capa tiempo real
        **Carpeta codigo/spark_proj/src/main/org.uam.masterbigdata**. Contiene los objetos con las funciones usadas para componer las transformaciones. Existen pruebas unitarias para cada una de ellas.
        - **ClassifierHelper**. Contiene el pipeline del modelo de ML. En su prueba unitaria correspodiente está el proceso de entrenamiento.
  - **Carpeta codigo/web**: Contiene el código de la web de consulta.

# Scripts

Todos los scripts para inicializar el entorno y realizar pruebas se encuentran en la raiz.

- **script_3_assembly_spark.sh**. Borra previos ensamblados del proyecto, lanza los tests y crea finalmente el ensamblado. El ensamblado se copia en la carpeta del entorno para poder ejecutarlo en el entorno de pruebas.
- **script_2_inicio_entorno.sh**. Arranca inicializa el docker compose con todo el entorno de pruebas.
  - El fichero docker-compose.yml se encuentra en la carpeta entorno.
- **script_3_inicio_web.sh**. Ensambla la web y la levanta.
- **script_4_batch.sh**. Lanza en el entorno de pruebas las tareas de la capa batch.
- **script_5_stream.sh**. Lanza en el entorno de pruebas las tareas de la capa de tiempo real.
- **script_6_stream_producer.sh**. Publica en el Kafka del entorno de pruebas para que la capa de tiempo real lo pueda consumir.

Importante: El docker-compose no se ha parado en ningún momento, es responsabilidad nuestra hacerlo.

# Ejecución

Ejecutar los scripts del 1 al 6 en orden.
El script 1 es opcional si ya se creo previamente el ensablado del proyecto Spark
El script 3 es opcional si ya se creo previamente el ensablado del proyecto Web,pero habría que ejecutar manualmente el ensamblado del proyecto (se puede ver en el script como hacerlo).

La web se puede consultar en: http://localhost:8080/api/v1.0/docs/index.html?url=/api/v1.0/docs/docs.yaml

Inicialización:
Podemos usar el sript "script_2_inicio_web.sh" o hacerlo manualmente.

Como vehículo de referencia usamos 763738942838145024

# Test unitarios

Todas las funciones
