package com.dcj.kafka.producer;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MyProducer {
    private static Logger log = LoggerFactory.getLogger(MyProducer.class);
    private final String CONF_PROPERTIES_FILE = "conf.properties";
    private final String PRODUCER_PROPERTIES_FILE = "producer.properties";

    private Properties confProps;
    private final int confReloadIntervalS = 1;

    private Properties kafkaProps;
    private Producer<String,String> producer;

    public MyProducer() throws Exception {
        confProps = new Properties();
        confProps.load(new FileInputStream(CONF_PROPERTIES_FILE));
        log.info("confProps:{}", confProps.toString());

        kafkaProps = new Properties();
        kafkaProps.load(new FileInputStream(PRODUCER_PROPERTIES_FILE));
        log.info("kafkaProps:{}", kafkaProps.toString());

        producer = new KafkaProducer<>(kafkaProps);
    }

    public void produce() throws InterruptedException {
        long lastConfReloadS = 0;
        long confLastModifyMill = 0;

        long lastSendMicro = 0;
        int curSendSize = 0;
        int totalSend = 0;

        String topic = "topic_test";
        String key = "test_";
        String msg = "1";

        int count = 0;
        while (true) {
            long nowMicro = System.currentTimeMillis() / 1000;
            long nowS = nowMicro / 1000;

            if (nowS > lastConfReloadS && nowS - lastConfReloadS > confReloadIntervalS ) {
                try {
                    File file = new File(CONF_PROPERTIES_FILE);
                    long tmpConfModifyMill = file.lastModified();
                    if (tmpConfModifyMill > confLastModifyMill) {
                        confLastModifyMill = tmpConfModifyMill;

                        confProps.load(new FileInputStream(file));
                        log.info("confProps:{}", confProps.toString());

                        topic = confProps.getProperty("topicName", topic);

                        int tmpTotalSend = Integer.parseInt(confProps.getProperty("totalSend", "10"));
                        if (totalSend != tmpTotalSend) {
                            count = 0;
                            totalSend = tmpTotalSend;
                        }

                        int tmpSize = Integer.parseInt(confProps.getProperty("msgSize", "1024"));
                        if (curSendSize != tmpSize) {
                            curSendSize = tmpSize;

                            char[] chars = new char[curSendSize];
                            Arrays.fill(chars, '1');
                            msg = new String(chars);
                            // value = IntStream.range(1, count).mapToObj(index -> "" + c).collect(Collectors.joining())
                        }
                    }
                } catch (FileNotFoundException e) {
                    //println(e.getMessage());
                } catch (IOException e) {

                } finally {
                    lastConfReloadS = nowS;
                }
            }

            int sendSpec = Integer.parseInt(confProps.getProperty("specIntervalMicro", "100"));
            int qps = Integer.parseInt(confProps.getProperty("qps", "100"));
            int sendCount = qps / sendSpec;
            if (nowMicro > lastSendMicro && nowMicro - lastSendMicro > sendSpec) {
                try {
                    for (int i = 0; i < sendCount; i++) {
                        key += Integer.toString(i);
                        producer.send(new ProducerRecord<>(topic, key, msg)).get();

                        count++;

                        if (totalSend > 0 && count >= totalSend) {
                            log.info("already send {} msg then return.", totalSend);
                            return;
                        }
                    }
                } catch (InterruptedException | ExecutionException e) {
                    log.error("msg:", e);
                } finally {
                    lastSendMicro = nowMicro;

                    continue;
                }
            }

            TimeUnit.MILLISECONDS.sleep(10);
        }
    }
}
