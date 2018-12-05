package com.martinywwan.launcher;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.stereotype.Component;
import scala.Tuple3;

import java.util.*;

//https://www.programcreek.com/java-api-examples/?class=org.apache.kafka.clients.consumer.KafkaConsumer&method=commitSync

@Component
public class KafkaLauncher {

    private KafkaConsumer<String, String> consumer = getKafkaConsumer();

    public void call() {
        System.out.println("Call");
        Map<TopicPartition, OffsetAndMetadata> metaData1 = new HashMap<>();
        Map<TopicPartition, OffsetAndMetadata> metaData12 = new HashMap<>();
        List<Tuple3<String, Integer, Long>> positionsToPersist= new ArrayList<>();
        List<PartitionInfo> partitionInfoList = consumer.partitionsFor("topic");
        partitionInfoList.forEach(partitionInfo -> System.out.println("Topic Consumer partition info :: " + partitionInfo.partition()));
        consumer.subscribe(Collections.singleton("test"));
        ConsumerRecords<String, String> consumerRecords = consumer.poll(30000);
        if (!consumerRecords.isEmpty()) {
            consumerRecords.forEach(record -> {
//                System.out.println(" " + record.);
                System.out.println("topic : " + record.topic() + " :: " + " partition :: " + record.partition() + " value :: " + record.value() +
                        " new position :: " + consumer.position(new TopicPartition(record.topic(), record.partition())));
            });
            System.out.println("position : " + consumerRecords.count());
            consumerRecords.partitions().forEach(topicPartition -> {
                System.out.println("New consumer partition :: " + topicPartition.partition() + " -- " + topicPartition.toString());
            });
            System.out.println("*****************************");
            //Tuple3<Topic, Partition, Position>
            consumerRecords.partitions().forEach(topicPartition -> {
                System.out.println("Results :: " + " topic :: " + topicPartition.topic() + " partition :: " + topicPartition.partition()
                + " topic meta data ::  "  + consumer.committed(new TopicPartition(topicPartition.topic(), topicPartition.partition())));
                positionsToPersist.add(Tuple3.apply(topicPartition.topic(), topicPartition.partition(),
                        consumer.position(new TopicPartition(topicPartition.topic(), topicPartition.partition()))));
                metaData1.put(new TopicPartition(topicPartition.topic(), topicPartition.partition()),
                        new OffsetAndMetadata(consumer.position(new TopicPartition(topicPartition.topic(), topicPartition.partition()))));
            });
        }
        positionsToPersist.forEach(stringIntegerLongTuple3 -> System.out.println("To persist :: " + stringIntegerLongTuple3.toString()));

        System.out.println("***************************");

        metaData1.entrySet().forEach(entry -> System.out.println("final :: " + entry.toString()));

//        consumer.committed();

        System.out.println("============================================");
        System.out.println("============================================");

        consumer.subscribe(Collections.singleton("wan"));
        ConsumerRecords<String, String> consumerRecords2 = consumer.poll(30000);
        if (!consumerRecords2.isEmpty()) {
            consumerRecords2.forEach(cr -> {
                System.out.println("topic : " + cr.topic() + " :: " + " partition :: " + cr.partition() + " value :: " + cr.value() +
                        " new position :: " + consumer.position(new TopicPartition(cr.topic(), cr.partition())));
            });
        }
        consumerRecords2.partitions().forEach(topicPartition -> {
            System.out.println("Results :: " + " topic :: " + topicPartition.topic() + " partition :: " + topicPartition.partition()
                    + " topic meta data ::  "  + consumer.committed(new TopicPartition(topicPartition.topic(), topicPartition.partition())));
            positionsToPersist.add(Tuple3.apply(topicPartition.topic(), topicPartition.partition(),
                    consumer.position(new TopicPartition(topicPartition.topic(), topicPartition.partition()))));
            metaData12.put(new TopicPartition(topicPartition.topic(), topicPartition.partition()),
                    new OffsetAndMetadata(consumer.position(new TopicPartition(topicPartition.topic(), topicPartition.partition()))));
        });

        metaData12.entrySet().forEach(entry -> System.out.println("final :: " + entry.toString()));


        consumer.commitSync(metaData1);
        consumer.commitSync(metaData12);


    }


    private void commitOffset() {

    }

    public KafkaConsumer<String, String> getKafkaConsumer() {
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, "testid13");
        kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        kafkaParams.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return new KafkaConsumer<>(kafkaParams);
    }
}
