package com.chen.guo.kafka;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.WakeupException;

import java.util.Arrays;
import java.util.Properties;

public class MyKafkaConsumer {
  public static void main(String[] args) {

    Properties prop = new Properties();
    prop.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
    prop.put("ssl.secure.random.implementation", "SHA1PRNG");
    prop.put("ssl.truststore.type", "JKS");
    prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "ltx1-kafka-kafka-queuing-vip.stg.linkedin.com:16637");
    prop.put("ssl.keystore.password", "work_around_jdk-6879539");
    prop.put("ssl.truststore.location", "/etc/riddler/cacerts");
    prop.put("ssl.truststore.password", "changeit");
    prop.put("ssl.keystore.type", "pkcs12");
    //prop.put("kafka.schemaRegistry.switchName","true");
    prop.put("kafka.schemaRegistry.url", "http://ltx1-schemaregistry-vip-2.stg.linkedin.com:10252/schemaRegistry/schemas");
    prop.put("ssl.keystore.location", "/Users/chguo/Downloads/identity.p12");
    prop.put("ssl.keymanager.algorithm", "SunX509");
    prop.put("client.id", "gobblin");
    prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
    //prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
    prop.put("ssl.trustmanager.algorithm", "SunX509");
    prop.put("ssl.key.password", "work_around_jdk-6879539");

    KafkaConsumer<String, String> kafkaConsumer;
    String topicName = "GobblinTrackingEvent_k2_chguo";
    String groupId = "g1";
    prop.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    prop.put(ConsumerConfig.CLIENT_ID_CONFIG, "simple");


    kafkaConsumer = new KafkaConsumer<>(prop);
    kafkaConsumer.subscribe(Arrays.asList(topicName));

    try {

      while (true) {

        ConsumerRecords<String, String> recs = kafkaConsumer.poll(100);
        for (ConsumerRecord<String, String> rec : recs)
          System.out.println(rec.value());

      }

    } catch (WakeupException ex) {

      System.err.println(ex.getMessage());

    } finally {

      kafkaConsumer.close();

    }
  }
}
