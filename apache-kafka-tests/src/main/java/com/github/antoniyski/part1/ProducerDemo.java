package com.github.antoniyski.part1;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerDemo {

	public static void main(String[] args) {

		// Producer properties
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		// Producer
		KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

		// ProducerRecord
		ProducerRecord<String, String> record = new ProducerRecord<String, String>("five_part_topic", "Message 1");

		// sending data
		producer.send(record);
		producer.flush();
		producer.close();

	}
}
