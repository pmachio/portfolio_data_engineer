#!/bin/bash
#Arranca un productor de kafka al topic test
sh /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:9092 --topic test