#!/bin/bash
#El primer parametro que se la pasa a la clase es la url donde se encuentran los datos, el segundo es la url con el modelo de clasificación guardado
docker exec -it entorno_spark-worker_1 /spark/bin/spark-submit --class org.uam.masterbigdata.drivers.ToSubmitDriver --deploy-mode client --master spark://spark-master:7077 --verbose --supervise /opt/spark-apps/spark_project.jar /opt/spark-data/datos.json /opt/spark-data/journeys_logreg_cv 

