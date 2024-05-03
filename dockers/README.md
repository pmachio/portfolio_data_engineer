# Intro

Imagenes de Dockers creadas para poder hacer pruebas de integracion

- [Schena Registry](#schema-registry). Contiene una copia del codigo del proyecto de Hortonworks, y el dockerfile para crear una imagen que podamos usar
- [Nifi](#nifi). Dockerfile para crear un nifi con el vim instalado para que podemoas crear archivos

## Schema Registry

El directorio hortonworks_schema_registry contiene el proyecto de [Registry de horton works](https://registry-project.readthedocs.io/en/latest/schema-registry.html).

El zip de se descargo de su [github](https://github.com/hortonworks/registry/releases)

Contiene el dockerfile para crear un Hortonworks Schema Registry que almacena los esquemas en memoria

## Nifi

Dockerfile para crear un nifi con el vim instalado para que podemoas crear archivos
