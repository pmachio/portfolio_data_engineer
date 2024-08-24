# Introducción

## HBASE
Script de creación de la tabla:
Entrar en contenedor de Hbase y cargar los datos mediante la shell
```HBase Shell
hbase shell

create 'default:WAR_PLAN', {NAME => 'cavalry', VERSIONS => 5}, {NAME => 'infantry', VERSIONS => 5}
put 'default:WAR_PLAN',1,'cavalry:number',65
put 'default:WAR_PLAN',2,'infantry:number',35
put 'default:WAR_PLAN',3,'infantry:number',50
put 'default:WAR_PLAN',3,'cavalry:number',50
```

## Importante
### Errores
Usar JDK 1.8

### Dockers
#### Hbase 
Hay que meter tanto el nombre del servicio de (por ejemplo hbase-docker), como
el nombre que máquina que se asigna a si mismo (podemos verlo en http://localhost:16010/master-status) en el fichero hosts apuntando a 127.0.0.1

## Referencias
Basado en
* HBASE
  * [Articulo sobre HBase y Spark](https://medium.com/nerd-for-tech/spark-read-from-write-to-hbase-table-using-dataframes-5c3b585c161)
  * [Referencias de Cloudera](https://community.cloudera.com/t5/Community-Articles/How-to-integrate-Spark3-with-HBase/ta-p/371238)
  * [Docker image](https://hub.docker.com/r/dajobe/hbase)
* KUDU
  * [Ejemplo sobre Kudu-Spark](https://github.com/apache/kudu/tree/master/examples/scala/spark-example)
  * [Quick-start](https://github.com/apache/kudu/tree/master/examples/quickstart/impala)