package com.kafka.listener;

import com.kafka.alertAndRecommendation.AlertAndRecommendationServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RequestMapping("/kafka-api-v1")
@RestController
public class MessageController {

    @Autowired
    KafkaListeners listener;

    @Autowired
    AlertAndRecommendationServiceImpl alertAndRecommendationManager;

    @GetMapping("/messages")
    @ResponseBody
    public List<String> getMessages() {
        return listener.getMessages();
    }

    @GetMapping("/alerts")
    @ResponseBody
    public List<String> getAlerts(){
        List<String> alerts=alertAndRecommendationManager.getAlerts();
        return alerts;
    }

    @GetMapping("/recommendations")
    @ResponseBody
    public List<String> getRecommendations(){
        List<String> recommendations=alertAndRecommendationManager.getRecommendations();
        return recommendations;
    }
}
