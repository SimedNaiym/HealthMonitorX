package com.kafka.mysensor;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import javax.jws.WebService;
import java.util.Date;

@WebService(endpointInterface = "com.kafka.mysensor.Sensor")
@Service
public class PhSensor implements Sensor{
    public final String topicName;
    public double phValue;
    public Date date;
    public final String sensorType;
    public final String alertTopic;
    public final String unit;

    private final KafkaTemplate<String, String> kafkaTemplate;

    public PhSensor(KafkaTemplate<String, String> kafkaTemplate) {
        this.topicName = "ph";
        this.sensorType = "PH";
        this.unit = "pH";
        this.alertTopic="phAlert";
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
        return "{Date: "+this.date+", Sensor type: "+this.sensorType+", Value: "+String.format("%.2f",this.phValue)+", Unit: "+this.unit+"}";
    }
    public void generateData(){
        //generating some random value for ph from 3 to 14
        this.phValue=Math.random()*11+3;
        this.date=new Date();
    }

    public void captureData() {
        //generating the data
        generateData();

        //sending the data about the sensor and its information
        kafkaTemplate.send(topicName,toString());

        //send the ph value to the ph alert topic
        kafkaTemplate.send(alertTopic,String.format("%.2f",this.phValue));
        synchronized (Sensor.SHARED_TOPIC){
            kafkaTemplate.send(Sensor.SHARED_TOPIC,toString());
        }
    }
}
