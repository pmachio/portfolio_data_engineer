#!/bin/bash
#Script para la creacion de certificado autofirmado, el keystore y truststore del server
#Basado en https://unix.stackexchange.com/questions/347116/how-to-create-keystore-and-truststore-using-self-signed-certificate
##
## SERVER
##
#Generate a private RSA key
openssl genrsa -out diagserverCA.key 2048 

#Create a x509 certificate
openssl req -x509 -new -nodes -key diagserverCA.key -sha256 -days 1024 -out diagserverCA.pem

##
## Client
##
#Generate a private RSA key
openssl genrsa -out diagclientCA.key 2048

#Create a x509 certificate
openssl req -x509 -new -nodes -key diagclientCA.key -sha256 -days 1024 -out diagclientCA.pem