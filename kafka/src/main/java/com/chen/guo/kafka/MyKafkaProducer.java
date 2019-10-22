package com.chen.guo.kafka;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

public class MyKafkaProducer {
  public static void main(String[] args) {

    String topicName = "GobblinTrackingEvent_k2_chguo";

    Properties props2 = new Properties();
    props2.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
    props2.put("ssl.secure.random.implementation", "SHA1PRNG");
    props2.put("ssl.truststore.type", "JKS");
    props2.put("bootstrap.servers", "ltx1-kafka-kafka-queuing-vip.stg.linkedin.com:16637");
    props2.put("ssl.keystore.password", "work_around_jdk-6879539");
    props2.put("ssl.truststore.location", "/etc/riddler/cacerts");
    props2.put("ssl.truststore.password", "changeit");
    props2.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    //props2.put("value.serializer", "com.chen.guo.kafka.JavaSerializer");
    props2.put("ssl.keystore.type", "pkcs12");
    //props2.put("kafka.schemaRegistry.switchName","true");
    props2.put("kafka.schemaRegistry.url", "http://ltx1-schemaregistry-vip-2.stg.linkedin.com:10252/schemaRegistry/schemas");
    props2.put("ssl.keystore.location", "/Users/chguo/Downloads/identity.p12");
    props2.put("ssl.keymanager.algorithm", "SunX509");
    props2.put("client.id", "gobblin");
    props2.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props2.put("ssl.trustmanager.algorithm", "SunX509");
    props2.put("ssl.key.password", "work_around_jdk-6879539");

    KafkaProducer<String, String> producer = new KafkaProducer<>(props2);
    Callback callback = new Callback() {
      @Override
      public void onCompletion(RecordMetadata metadata, Exception exception) {
        System.out.println("Sent");
      }
    };
    for (long i = 0; i < 10; i++) {
      ProducerRecord<String, String> data = new ProducerRecord<>(
          topicName, "key-" + i, "message-" + i);
      producer.send(data, callback);
    }

    producer.close();
  }
}
