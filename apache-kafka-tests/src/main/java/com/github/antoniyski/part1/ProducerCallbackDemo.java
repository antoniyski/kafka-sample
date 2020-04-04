package com.github.antoniyski.part1;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerCallbackDemo {

	public static void main(String[] args) {

		Logger logger = LoggerFactory.getLogger(ProducerCallbackDemo.class);

		// Producer properties
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		// Producer
		KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

		for (int i = 0; i < 10; i++) {
			// ProducerRecord
			ProducerRecord<String, String> record = new ProducerRecord<String, String>("five_part_topic",
					"Message " + i);

			// sending data
			producer.send(record, new Callback() {

				@Override
				public void onCompletion(RecordMetadata metadata, Exception exception) {
					logger.info("\nTopic name = " + metadata.topic() + "\n" + "Partition = " + metadata.partition()
							+ "\n" + "Timestamp = " + metadata.timestamp());
				}
			});
			producer.flush();
		}

		
		producer.close();

	}
}
