package com.kafka.mysensor;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import javax.jws.WebService;
import java.util.Date;

@WebService(endpointInterface = "com.kafka.mysensor.Sensor")
@Service
public class OxygenSaturationSensor implements Sensor {
    public final String topicName;
    public double oxygenSaturationValue;
    public final String sensorType;
    public final String unit;
    public final String alertTopic;
    public Date date;

    private final KafkaTemplate<String, String> kafkaTemplate;

    public OxygenSaturationSensor(KafkaTemplate<String, String> kafkaTemplate) {
        this.topicName = "oxygensaturation";
        this.sensorType = "OXYGEN_SATURATION";
        this.alertTopic = "oxygensaturationAlert";
        this.unit = "%";
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
        return "{Date: " + this.date + ", Sensor type: " + this.sensorType + ", Value: " + String.format("%.2f", this.oxygenSaturationValue) + " " + this.unit + "}";
    }

    public void generateData() {
        this.oxygenSaturationValue = 90 + Math.random() * 10; // 90% -> 100%
        this.date = new Date();
    }

    public void captureData() {
        // Generating the data
        generateData();
        kafkaTemplate.send(topicName, toString());
        kafkaTemplate.send(alertTopic, String.format("%.2f", this.oxygenSaturationValue));
        synchronized (Sensor.SHARED_TOPIC) {
            kafkaTemplate.send(Sensor.SHARED_TOPIC, toString());
        }
    }
}
