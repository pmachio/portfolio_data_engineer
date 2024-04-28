FROM apache/nifi-registry:0.8.0

COPY server.keystore /opt/nifi-registry/nifi-registry-current/conf/keystore.jks
COPY server.truststore /opt/nifi-registry/nifi-registry-current/conf/truststore.jks
COPY diagserverCA.pem /opt/certs/cert.pem

#Pasar por variables en el compose
#COPY nifi-registry.properties /opt/nifi-registry/nifi-registry-current/conf/nifi-registry.properties