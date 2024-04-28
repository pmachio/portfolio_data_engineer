#!/bin/bash
#Script para la creacion de certificado autofirmado, el keystore y truststore del server
#Basado en https://unix.stackexchange.com/questions/347116/how-to-create-keystore-and-truststore-using-self-signed-certificate

#Create a PKCS12 keystore from private key and public certificate.
openssl pkcs12 -export -name server-cert -in diagserverCA.pem -inkey diagserverCA.key -out serverkeystore.p12

#Convert PKCS12 keystore into a JKS keystore
keytool -importkeystore -destkeystore server.keystore -srckeystore serverkeystore.p12 -srcstoretype pkcs12 -alias server-cert

#Import a client's certificate to the server's trust store.
keytool -import -alias client-cert -file diagclientCA.pem -keystore server.truststore

#Import a server's certificate to the server's trust store.
keytool -import -alias server-cert -file diagserverCA.pem -keystore server.truststore