#!/bin/bash
#Mueve los certificados, keystore y truststore a sus repectivos directorios para la creación de la imagen de docker
#Nifi, que lo veremos como el cliente
mv client.keystore ../nifi_docker_build
mv client.truststore ../nifi_docker_build
mv diagclientCA.pem ../nifi_docker_build
rm diagclientCA.key
rm clientkeystore.p12

#Nifi, que lo veremos como el cliente
mv server.keystore ../nifi_registry_docker_build
mv server.truststore ../nifi_registry_docker_build
mv diagserverCA.pem ../nifi_registry_docker_build
rm diagserverCA.key
rm serverkeystore.p12