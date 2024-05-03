#Basado en la documentacion para centos https://registry-project.readthedocs.io/en/latest/install.html#centos-redhat
FROM oraclelinux:8

#hubo problemas con mirror sync

#bajar la version registry
RUN yum install -y wget
#RUN apt-get install wget

RUN wget https://github.com/hortonworks/registry/releases/download/1.0.0-rc2/schemaregistry-1.0.0.zip

RUN yum install -y unzip
RUN unzip schemaregistry-1.0.0.zip

#Establacer los jar en 

#configurar registry.yaml


#COPY schema_registry_db.sql .

#RUN mysql -u root -p schema_registry_db.sql
#RUN mysql  schema_registry_db.sql
#ADD schema_registry_db.sql /docker-entrypoint-initdb.d

#No es seguro pero vale para hacer las pruebas
RUN yum install -y java-11-openjdk

#Descargar los conectroes
#RUN wget https://downloads.mysql.com/archives/get/p/3/file/mysql-connector-j-8.2.0.tar.gz
#RUN tar -xvzf mysql-connector-j-8.2.0.tar.gz
#RUN cp mysql-connector-j-8.2.0/mysql-connector-j-8.2.0.jar /schemaregistry-1.0.0/libs/
#RUN cp mysql-connector-j-8.2.0/mysql-connector-j-8.2.0.jar /schemaregistry-1.0.0/bootstrap/lib
#Configurar para usar mysql
RUN cp /schemaregistry-1.0.0/conf/registry-inmemory-example.yaml /schemaregistry-1.0.0/conf/registry.yaml


EXPOSE 9090/TCP

#Cambiarlos por el arranque de registry
#ENTRYPOINT [ "/schemaregistry-1.0.0/bin/registry", "start" ]
ENTRYPOINT [ "/schemaregistry-1.0.0/bin/registry-server-start.sh", "/schemaregistry-1.0.0/conf/registry.yaml" ]
