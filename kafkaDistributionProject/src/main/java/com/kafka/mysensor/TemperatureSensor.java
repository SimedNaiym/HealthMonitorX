package com.kafka.mysensor;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import javax.jws.WebService;
import java.util.Date;

@WebService(endpointInterface = "com.kafka.mysensor.Sensor")
@Service
public class TemperatureSensor implements Sensor {
    public final String topicName;
    public double temperatureValue;
    public final String sensorType;
    public final String unit;
    public final String alertTopic;
    public Date date;

    private final KafkaTemplate<String, String> kafkaTemplate;

    public TemperatureSensor(KafkaTemplate<String, String> kafkaTemplate) {
        this.topicName = "temperature";
        this.sensorType = "TEMPERATURE";
        this.alertTopic = "temperatureAlert";
        this.unit = "°C";
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public String getData() {
        return this.toString();
    }

    @Override
    public String getSensorType() {
        return sensorType;
    }

    @Override
    public String toString() {
        return "{Date: " + this.date + ", Sensor type: " + this.sensorType + ", Value: " + String.format("%.2f", this.temperatureValue) + " " + this.unit + "}";
    }

    public void generateData() {
        this.temperatureValue = 30 + Math.random() * 9.7; // Generates temperature values between 30 and 39.7°C
        this.date = new Date();
    }


    public void captureData() {
        // Generating the data
        generateData();
        kafkaTemplate.send(topicName, toString());
        kafkaTemplate.send(alertTopic, String.format("%.2f", this.temperatureValue));
        synchronized (Sensor.SHARED_TOPIC) {
            kafkaTemplate.send(Sensor.SHARED_TOPIC, toString());
        }
    }
}
