package com;

import com.subomi.kafkaDemo.Publisher;

public class main {
    public static void main(String[] args) {
        Publisher publisher= new Publisher();
        publisher.publicEventToKafka("{\"id\":55,\"name\":\"subomi\",\"age\":20}");
    }
}
