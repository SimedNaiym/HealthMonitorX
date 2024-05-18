package com.kafka.alertAndRecommendation;

import javax.jws.WebMethod;
import javax.jws.WebResult;
import javax.jws.WebService;
import javax.jws.soap.SOAPBinding;
import java.util.List;

@WebService(name="AlertAndRecommendation")
@SOAPBinding(style= SOAPBinding.Style.RPC)
public interface AlertAndRecommendationService {
    @WebMethod(operationName = "alerts")
    @WebResult(name="alerts")
    List<String> getAlerts();
    @WebMethod(operationName = "recommendations")
    @WebResult(name="recommendations")
    List<String> getRecommendations();
}
