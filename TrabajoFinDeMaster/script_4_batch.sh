#!/bin/bash
#El primer parametro que se la pasa a la clase es la url donde se encuentran los datos, el segundo es la url con el modelo de clasificación guardado
docker exec -it $(docker-compose -f entorno/docker-compose.yml ps -q spark-worker) /spark/bin/spark-submit --class org.uam.masterbigdata.drivers.ToSubmitDriver --deploy-mode client --master spark://spark-master:7077 --supervise /opt/spark-apps/spark_project.jar /opt/spark-data/datos.json /opt/spark-data/journeys_logreg_cv 
