# Introducción

Ejemplo de uso de apache nifi y horton works basado en [esto](https://community.cloudera.com/t5/Community-Articles/Installing-a-local-Hortonworks-Registry-to-use-with-Apache/ta-p/246640) para publicar en kafka.

1. Levantar el entorno con docker compose usando el archivo docker-compose.yml.

2. Cargar el esquema usando schema.json

- El nombre del esquema es 'users'

- El grupo del esquema es 'NiFi'

- El tipo es Avro schema provider

- El api mediante swagger esta en http://localhost:9090/swagger

3. Meterse en el docker de kafka para observar la producción con el clisnte kafka-console-consumer en el topic 'test'

4. Ejecutar el flujo de Nifi usando el template CSV_NIFI_HORTONREGISTRY_AVRO_KAFKA.xml

- Configurar los controler servicies. Habilitar el servicio HortonworksSchemaRegistry primero y luego el resto
- Para que el procesador 'Get CSV File' pueda producir hay que meterse en contenedor de nifi, y en el bash crear la carpeta 'Get CSV File' y dentro crear un csv que contenga los datos del archivo 'users.txt'

## Integracion Spark
