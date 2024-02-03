#!/bin/bash
#generar el jar
cd codigo
sbt web/clean
sbt web/test #aunque no hay tests
sbt web/assembly
cd ..
#arrancar la web
echo "URL de la web: http://localhost:8080/api/v1.0/docs/index.html?url=/api/v1.0/docs/docs.yaml"
java -jar codigo/web/target/scala-2.12/web.jar &

