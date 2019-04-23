package com.mpojeda84.mapr.connectedcarmaprtofirebase.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.mpojeda84.mapr.connectedcarmaprtofirebase.Application;
import com.mpojeda84.mapr.connectedcarmaprtofirebase.model.CarDataDto;
import com.mpojeda84.mapr.connectedcarmaprtofirebase.data.MapRDBDataAcess;
import com.mpojeda84.mapr.connectedcarmaprtofirebase.model.MessageDto;
import org.springframework.web.client.RestTemplate;

import java.util.List;

public class CarDataService {

    public void sendAllToFirebase(){
        System.out.println("Loding and Sending");

        //open connection
        MapRDBDataAcess.initializeMapRDBConnection();

        // load elements, that were written by vin number
        List<JsonNode> all = MapRDBDataAcess.loadAllFromMapRDB(Application.table);
        System.out.println("count:" + all.size());

        // load mesages from messages table
        List<JsonNode> messages = MapRDBDataAcess.loadAllMessagesFromMapRDB(Application.messagesTable);
        System.out.println("messages count:" + messages.size());

        //close
        MapRDBDataAcess.closeMapRDBConnection();

//        process the values to send to Firebase
        Double communityAverage = findCommunityAverage(all);
        all.stream() // number of elements in this stream is equals to the number of different VINs
                .map(CarDataHelper::process)
                .map(CarDataHelper::normalizeAndFormat)
                .map(x -> {
                    // add average data to each individual VIN
                    x.setAvgCommunitySpeed(CarDataHelper.toInt(communityAverage.toString()));
                    return x;
                })
                .forEach( this::sendToFirebase);

        CarDataHelper.toMessagesMap(messages).forEach(this::sendMessagesToFirebase);

    }

    private void sendMessagesToFirebase(String vin, List<MessageDto> messagesDto) {
        System.out.println("Messages for vin " + vin + ": ");
        messagesDto.stream().forEach(System.out::println);
        RestTemplate restTemplate = new RestTemplate();
        restTemplate.put(Application.firebase + "/messages/"+ vin +".json", CarDataHelper.serialize(messagesDto));
    }

    // execute the url with a POST and updates FIREBASE car data for each VIN number
    private void sendToFirebase(CarDataDto carDataDto) {
        System.out.println(CarDataHelper.serialize(carDataDto));
        // String utilioty to make HTTP calls
        RestTemplate restTemplate = new RestTemplate();
        restTemplate.put(Application.firebase + "/cars/"+ carDataDto.getVin() +".json", CarDataHelper.serialize(carDataDto));
    }

    private Double findCommunityAverage(List<JsonNode> cars) {
        // summing car total speeds (that are already summed) to then divide by the total amount of data points and get the agerage of the community
        Double speedTotal = cars.stream().map(x -> x.get("speedSum").asDouble(0.0)).reduce((x,y) -> x + y).orElse(Double.valueOf(0.0));
        Integer speedDataPoints = cars.stream().map(x -> x.get("dataPointCount").asInt(0)).reduce((x,y) -> x + y).orElse(0);
        return speedTotal / speedDataPoints;
    }

}
