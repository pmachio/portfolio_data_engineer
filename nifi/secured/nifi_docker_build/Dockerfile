FROM apache/nifi:1.13.2

COPY client.keystore /opt/nifi/nifi-current/conf/keystore.jks
COPY client.truststore /opt/nifi/nifi-current/conf/truststore.jks
COPY diagclientCA.pem /opt/certs/cert.pem

#Luego lo sobre escribe. No se como corregirlo para poder publicar como https
#COPY nifi.properties /opt/nifi/nifi-current/conf/nifi.properties