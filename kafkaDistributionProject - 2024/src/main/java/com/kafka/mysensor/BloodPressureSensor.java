package com.kafka.mysensor;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import javax.jws.WebService;
import java.util.Date;

@WebService(endpointInterface = "com.kafka.mysensor.Sensor")
@Service
public class BloodPressureSensor implements Sensor {
    public final String topicName;
    public double systolicPressure;
    public double diastolicPressure;
    public final String sensorType;
    public final String unit;
    public final String alertTopic;
    public Date date;

    private final KafkaTemplate<String, String> kafkaTemplate;

    public BloodPressureSensor(KafkaTemplate<String, String> kafkaTemplate) {
        this.topicName = "bloodpressure";
        this.sensorType = "BLOOD_PRESSURE";
        this.alertTopic = "bloodpressureAlert";
        this.unit = "mmHg";
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
        return "{Date: " + this.date + ", Sensor type: " + this.sensorType + ", Systolic Pressure: " + String.format("%.2f", this.systolicPressure) + " " + this.unit + "}";
    }

    public void generateData() {
        this.systolicPressure = 90 + Math.random() * 70; // 90 -> 160 mmHg
        //this.diastolicPressure = 60 + Math.random() * 40; // 60 -> 100 mmHg
        this.date = new Date();
    }

    public void captureData() {
        // Generating the data
        generateData();
        kafkaTemplate.send(topicName, toString());
        kafkaTemplate.send(alertTopic, "Systolic: " + String.format("%.2f", this.systolicPressure));
        synchronized (Sensor.SHARED_TOPIC) {
            kafkaTemplate.send(Sensor.SHARED_TOPIC, toString());
        }
    }
}
