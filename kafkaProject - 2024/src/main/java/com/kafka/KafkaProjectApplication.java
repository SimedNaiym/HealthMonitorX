package com.kafka;

import com.kafka.mysensor.*;
import jakarta.annotation.PostConstruct;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.ArrayList;
import java.util.List;

@SpringBootApplication(scanBasePackages = "com.kafka")
public class KafkaProjectApplication {

	public static void main(String[] args) {
		ConfigurableApplicationContext context = SpringApplication.run(KafkaProjectApplication.class, args);

		// Get the KafkaTemplate bean from the application context
		KafkaTemplate<String, String> kafkaTemplate = context.getBean(KafkaTemplate.class);

		// Create instances of each sensor
		TemperatureSensor temperatureSensor = new TemperatureSensor(kafkaTemplate);
		OxygenSaturationSensor oxygenSaturationSensor = new OxygenSaturationSensor(kafkaTemplate);
		HeartRateSensor heartRateSensor = new HeartRateSensor(kafkaTemplate);
		BloodPressureSensor bloodPressureSensor = new BloodPressureSensor(kafkaTemplate);

		// Starting the sensors and generating the data
		while (true) {
			temperatureSensor.captureData();
			oxygenSaturationSensor.captureData();
			heartRateSensor.captureData();
			bloodPressureSensor.captureData();
			try {
				// Sleeping the thread for 5 seconds
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				System.out.println(e.getMessage());
			}
		}
	}
}
