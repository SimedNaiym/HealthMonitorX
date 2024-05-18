package com.kafka.listener;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
public class KafkaListeners {
    public List<String> messages=new ArrayList<>();
    @KafkaListener(topics="sensors",groupId="groupId")
    public void listener(String data){
            messages.add(data);
    }

    public List<String> getMessages(){
        return this.messages;
    }
}
