# Secured

Con un script crearemos el certificado y los archivos de keysstore y truststore para realizar la comunicacion segura entre Nifi, Nifi-Regitry y Nifi-Toolkit
Usaremos dockers para contenerizar los 3 componentes y probar el ejemplo

## Creacion de los certificados, keysstore, truststore

[Referencia](https://unix.stackexchange.com/questions/347116/how-to-create-keystore-and-truststore-using-self-signed-certificate)

En la carpeta **_certifies_keystore_truststore_** hay 3 scripts para crear los certificados, keystore y truststore del Nifi-registry (server) y de Nifi (client). El 4 script es para moverlos a sus respectivos directorios y borrar la key de ambos

## Docker
