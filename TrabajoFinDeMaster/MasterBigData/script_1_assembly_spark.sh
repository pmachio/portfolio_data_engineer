#!/bin/bash
#crear el jar del projecto de spark. Contiene la parte batch y la de Stream
cd codigo
sbt spark_proj/clean
#El test no solo es importante pasarlo por motivos de calidad sino que también se entrena y guarda el modelo.
sbt spark_proj/test
sbt spark_proj/assembly
#copiar archivo jar
mv spark_proj/target/scala-2.12/spark_project.jar ../entorno/apps

