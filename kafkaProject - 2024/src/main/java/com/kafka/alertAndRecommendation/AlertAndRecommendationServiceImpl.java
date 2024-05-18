package com.kafka.alertAndRecommendation;

import com.kafka.config.KafkaConsumerConfig;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import javax.jws.WebService;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

@WebService(endpointInterface = "com.kafka.alertAndRecommendation.AlertAndRecommendationService")
@Service
public class AlertAndRecommendationServiceImpl implements AlertAndRecommendationService {

    private static final Logger LOGGER = Logger.getLogger(AlertAndRecommendationServiceImpl.class.getName());

    private String heartRateAlertMessage;
    private String bloodPressureAlertMessage;
    private String bodyTemperatureAlertMessage;
    private String oxygenSaturationAlertMessage;

    private String heartRateRecommendationMessage;
    private String bloodPressureRecommendationMessage;
    private String bodyTemperatureRecommendationMessage;
    private String oxygenSaturationRecommendationMessage;

    private List<String> alerts;
    private List<String> recommendations;

    private final KafkaConsumerConfig kcc;

    public AlertAndRecommendationServiceImpl() {
        this.kcc=new KafkaConsumerConfig();
    }

    @KafkaListener(topics="heartrateAlert", groupId="groupId")
    public String getHeartRateAlertAndRecommendation(String data) {
    /*
    """
    Les recommandations pour le rythme cardiaque dans le cas du diabète varient en fonction de l'état de santé du patient.
    """
    */
        Double value = 0.0;
        try {
            // Preprocess the data: Replace commas with dots for decimal parsing and trim whitespace
            data = data.replace(',', '.').trim();
            // Attempt to parse the cleaned data
            value = Double.valueOf(data);
        } catch (Exception e) {
            // Log the error with the raw data for debugging purposes
            LOGGER.log(Level.WARNING, "Error parsing heart rate value: " + data + ". Exception: " + e.getMessage());
        }

        // Logic for determining heart rate alerts and recommendations
        if (value < 60) {
            this.heartRateAlertMessage = "!!!- Rythme cardiaque trop bas";
            this.heartRateRecommendationMessage = "-> Le rythme cardiaque est trop bas:  a: Repos et relaxation.  b: Hydratation suffisante.";
        } else if (value > 100) {
            this.heartRateAlertMessage = "!!!- Rythme cardiaque trop élevé";
            this.heartRateRecommendationMessage = "-> Le rythme cardiaque est trop élevé:  a: Repos et relaxation.  b: Consultez un médecin.";
        } else {
            this.heartRateAlertMessage = "Rythme cardiaque dans la plage normale";
            this.heartRateRecommendationMessage = "-> Le rythme cardiaque est normal.";
        }

        // Print and return the alert and recommendation messages
        System.out.println("\n+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
        System.out.println(heartRateAlertMessage + "\n\"\"\"\n" + heartRateRecommendationMessage);
        return heartRateAlertMessage + "\n\"\"\"\n" + heartRateRecommendationMessage;
    }



    @KafkaListener(topics = "bloodpressureAlert", groupId = "groupId")
    public String getBloodPressureAlertAndRecommendation(String data) {
        Double value = 0.0;
        try {
            // Remove the prefix and trim the string
            if (data.startsWith("Systolic:")) {
                data = data.replace("Systolic:", "").trim();
            }
            value = Double.valueOf(data.replace(',', '.'));
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Error parsing blood pressure value: " + e.getMessage());
        }

        // Example logic for blood pressure alerts and recommendations
        if (value < 90) {
            this.bloodPressureAlertMessage = "!!!- Pression artérielle trop basse";
            this.bloodPressureRecommendationMessage = "-> La pression artérielle est trop basse:   a: Augmentez votre apport en sel.  b: Buvez plus d'eau.";
        } else if (value > 140) {
            this.bloodPressureAlertMessage = "!!!- Pression artérielle trop élevée";
            this.bloodPressureRecommendationMessage = "-> La pression artérielle est trop élevée:  a: Limitez votre consommation de sel.  b: Faites de l'exercice régulièrement.  c: Consultez un médecin.";
        } else {
            this.bloodPressureAlertMessage = "Pression artérielle dans la plage normale";
            this.bloodPressureRecommendationMessage = "-> La pression artérielle est normale.";
        }

        System.out.println("\n+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
        System.out.println(bloodPressureAlertMessage + "\n\"\"\"\n" + bloodPressureRecommendationMessage);
        return bloodPressureAlertMessage + "\n\"\"\"\n" + bloodPressureRecommendationMessage;
    }



    @KafkaListener(topics="temperatureAlert", groupId="groupId")
    public String getBodyTemperatureAlertAndRecommendation(String data) {
    /*
    """
    Les recommandations pour la température corporelle varient en fonction de plusieurs facteurs tels que l'âge, l'état de santé général et l'environnement.
    """
    */
        Double value = 0.0;
        try {
            // Preprocess the data: Replace commas with dots for decimal parsing and trim whitespace
            data = data.replace(',', '.').trim();
            // Attempt to parse the cleaned data
            value = Double.valueOf(data);
        } catch (Exception e) {
            // Log the error with the raw data for debugging purposes
            LOGGER.log(Level.WARNING, "Error parsing body temperature value: " + data + ". Exception: " + e.getMessage());
        }

        // Logic for determining body temperature alerts and recommendations
        if (value < 36.1) {
            this.bodyTemperatureAlertMessage = "!!!- Température corporelle trop basse";
            this.bodyTemperatureRecommendationMessage = "-> La température corporelle est trop basse:  a: Portez des vêtements chauds.  b: Restez à l'intérieur dans un endroit chaud.";
        } else if (value > 37.2) {
            this.bodyTemperatureAlertMessage = "!!!- Température corporelle trop élevée";
            this.bodyTemperatureRecommendationMessage = "-> La température corporelle est trop élevée:  a: Restez hydraté.  b: Prenez des médicaments pour réduire la fièvre.  c: Consultez un médecin si la fièvre persiste.";
        } else {
            this.bodyTemperatureAlertMessage = "Température corporelle normale";
            this.bodyTemperatureRecommendationMessage = "-> La température corporelle est normale.";
        }

        // Print and return the alert and recommendation messages
        System.out.println("\n+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
        System.out.println(bodyTemperatureAlertMessage + "\n\"\"\"\n" + bodyTemperatureRecommendationMessage);
        return bodyTemperatureAlertMessage + "\n\"\"\"\n" + bodyTemperatureRecommendationMessage;
    }


    @KafkaListener(topics="oxygensaturationAlert", groupId="groupId")
    public String getOxygenSaturationAlertAndRecommendation(String data) {
    /*
    """
    Les recommandations pour la saturation en oxygène varient en fonction de l'état de santé du patient et de l'environnement.
    """
    */
        Double value = 0.0;
        try {
            // Preprocess the data: Replace commas with dots for decimal parsing and trim whitespace
            data = data.replace(',', '.').trim();
            // Attempt to parse the cleaned data
            value = Double.valueOf(data);
        } catch (Exception e) {
            // Log the error with the raw data for debugging purposes
            LOGGER.log(Level.WARNING, "Error parsing oxygen saturation value: " + data + ". Exception: " + e.getMessage());
        }

        // Logic for determining oxygen saturation alerts and recommendations
        if (value < 90.0) {
            this.oxygenSaturationAlertMessage = "!!!- Saturation en oxygène trop basse";
            this.oxygenSaturationRecommendationMessage = "-> La saturation en oxygène est trop basse:  a: Respirez profondément et lentement.  b: Utilisez un concentrateur d'oxygène si disponible.  c: Consultez un médecin si la saturation reste basse.";
        } else if (value > 100.0) {
            this.oxygenSaturationAlertMessage = "!!!- Saturation en oxygène anormalement élevée";
            this.oxygenSaturationRecommendationMessage = "-> La saturation en oxygène est anormalement élevée:  a: Vérifiez l'équipement de mesure.  b: Consultez un médecin pour évaluer si un traitement est nécessaire.";
        } else {
            this.oxygenSaturationAlertMessage = "Saturation en oxygène normale";
            this.oxygenSaturationRecommendationMessage = "-> La saturation en oxygène est normale.";
        }

        // Print and return the alert and recommendation messages
        System.out.println("\n+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
        System.out.println(oxygenSaturationAlertMessage + "\n\"\"\"\n" + oxygenSaturationRecommendationMessage);
        return oxygenSaturationAlertMessage + "\n\"\"\"\n" + oxygenSaturationRecommendationMessage;
    }



    @Override
    public List<String> getAlerts() {
        alerts = new ArrayList<>();
        alerts.add(heartRateAlertMessage);
        alerts.add(bloodPressureAlertMessage);
        alerts.add(bodyTemperatureAlertMessage);
        alerts.add(oxygenSaturationAlertMessage);
        return alerts;
    }

    @Override
    public List<String> getRecommendations() {
        recommendations = new ArrayList<>();
        recommendations.add(heartRateRecommendationMessage);
        recommendations.add(bloodPressureRecommendationMessage);
        recommendations.add(bodyTemperatureRecommendationMessage);
        recommendations.add(oxygenSaturationRecommendationMessage);
        return recommendations;
    }

}
