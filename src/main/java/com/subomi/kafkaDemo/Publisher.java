package com.subomi.kafkaDemo;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.mycorp.mynamespace.sampleRecord;
import com.subomi.kafkaDemo.model.ResponseData;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.json.JsonString;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.UUID;

import static com.subomi.kafkaDemo.utility.LoadKafkaProperties.producerProperties;

public class Publisher {
    public void publicEventToKafka(String request) throws IOException {
        sampleRecord itemWithOwner = new ObjectMapper().readValue(request, sampleRecord.class);
        ResponseData  responseData= new ResponseData();
        boolean response= publishEventToKafkaHelper(itemWithOwner);
        if(response==true)
        {
            responseData.setMessage("Event was published to Kafka");

        }
        else{
            responseData.setMessage("Failed to published event to Kafka");

        }
    }
    private boolean publishEventToKafkaHelper(sampleRecord request)  {


        try {
            Producer<String, sampleRecord> producer = new KafkaProducer<>(producerProperties());
            producer.send(new ProducerRecord<>("topic_0", generateKey(), request));
            producer.close();
           return true;
        }
        catch (Exception e) {
         return false;
        }
    }

    private String generateKey(){
        return UUID.randomUUID().toString();
    }
    private Integer generateId(){
        return (int)(Math.random()*100);
    }
}
