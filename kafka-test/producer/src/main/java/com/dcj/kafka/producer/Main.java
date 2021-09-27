package com.dcj.kafka.producer;


import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
    private static final Logger log = LoggerFactory.getLogger(Main.class);
    public static void main(String[] args) {
        try {
            MyProducer myProducer = new MyProducer();
            myProducer.produce();
        } catch (Exception e) {
            log.error("stack:", e);
        }
    }
}
