package com.dcj.kafka.producer;

import java.util.Properties;

public class Main {
    public static void main(String[] args) {
        try {
            MyProducer myProducer = new MyProducer();
            myProducer.produce();
        } catch (Exception e) {

        }
    }
}
