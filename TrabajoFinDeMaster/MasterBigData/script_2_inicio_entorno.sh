#!/bin/bash
#arrancar docker compose
docker-compose -f entorno/docker-compose.yml up -d
#No muy ortodoxo pero le damos un tiempo a que arranque
sleep 10s

echo "RECUERDA PARAR EL DOCKER COMPOSE DE ENTORNO AL TERMINAR"
