package org.pmachio.spark.metrics.influxdb;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.WriteApi;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

//Ejemplo
// https://github.com/iZettle/dropwizard-metrics-influxdb/blob/master/metrics-influxdb/src/main/java/com/izettle/metrics/influxdb/InfluxDbSender.java
public class InfluxDbSender {
    private static final Logger LOGGER = LoggerFactory.getLogger(InfluxDbSender.class);
    private final InfluxDBClient client;
    private final String bucket;
    private final String org;

    public  InfluxDbSender(String url, String token, String bucket, String org) {
        this.bucket = bucket;
        this.org = org;
        client = InfluxDBClientFactory.create(url, token.toCharArray());
    }

    /*Bucket tienen un periorodo de duracion. Son el nivel más alto de almacenamiento que permite ordernar según nuestro criterois
    Measuramentes euivales a tabla. Tienen
    * Point as una fila y tie
    Tags y fields corresponden a columnas
    *
    * El buccket es la base de datos*/
    public void writePointMeasument(String name, Long now, Map<String, Object> fields){
        Point point = Point
                .measurement(name)
                .addTag("host", "host1")
                .addFields(fields)
                //OJO es PELIGROSO, si loe stablecemos mal podemos nbo ver datos
                .time(now, WritePrecision.MS);
        try (WriteApi writeApi = client.getWriteApi()) {
            writeApi.writePoint(bucket, org, point);
            LOGGER.warn("WRITTED" + name + " -" + fields.toString() + " - " + now.toString() + "NS" );
        }catch (Exception e){
            LOGGER.error("ERROR al escribir" + e.getMessage());
        }
    }
}
