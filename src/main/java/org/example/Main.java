package org.example;

public class Main {
    public static void main(String[] args) {
        MyKafkaProducer myKafkaProducer = new MyKafkaProducer();
        MyKafkaConsumer myKafkaConsumer = new MyKafkaConsumer();

        Thread producer = new Thread(myKafkaProducer);
        Thread consumer = new Thread(myKafkaConsumer);

        producer.start();
        consumer.start();
    }
}
