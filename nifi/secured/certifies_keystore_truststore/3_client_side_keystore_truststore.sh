#!/bin/bash
#Script para la creacion de certificado autofirmado, el keystore y truststore del server
#Basado en https://unix.stackexchange.com/questions/347116/how-to-create-keystore-and-truststore-using-self-signed-certificate

#Create PKCS12 keystore from private key and public certificate.
openssl pkcs12 -export -name client-cert -in diagclientCA.pem -inkey diagclientCA.key -out clientkeystore.p12

#Convert a PKCS12 keystore into a JKS keystore
keytool -importkeystore -destkeystore client.keystore -srckeystore clientkeystore.p12 -srcstoretype pkcs12 -alias client-cert

#Import a server's certificate to the client's trust store.
keytool -import -alias server-cert -file diagserverCA.pem -keystore client.truststore

#Import a client's certificate to the client's trust store.
keytool -import -alias client-cert -file diagclientCA.pem -keystore client.truststore