package com.github.antoniyski.part1;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemo {

	public static void main(String[] args) {

		Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);
		String bootstrapServer = "localhost:9092";
		String consumerGroup = "consumer-group-2";
		String topic = "five_part_topic";

		// properties
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");

		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

		// subscribing to topic(s)
		consumer.subscribe(Collections.singleton(topic));

		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
			for (ConsumerRecord<String, String> record : records) {
				System.out.println(
						"Key: " + record.key() + " Partition: " + record.partition() + " Offset" + record.offset());
			}
		}

	}

}
