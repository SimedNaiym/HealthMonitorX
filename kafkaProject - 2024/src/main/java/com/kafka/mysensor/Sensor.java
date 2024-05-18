package com.kafka.mysensor;

import javax.jws.WebMethod;
import javax.jws.WebResult;
import javax.jws.WebService;
import javax.jws.soap.SOAPBinding;

@WebService(name="Sensor",serviceName = "SensorService")
@SOAPBinding(style= SOAPBinding.Style.RPC)
public interface Sensor {
    @WebMethod(operationName = "data")
    @WebResult(name="data")
    String getData() ;
    @WebMethod(operationName = "sensorType")
    @WebResult(name="sType")
    String getSensorType();

    String SHARED_TOPIC= "sensors" ;
}