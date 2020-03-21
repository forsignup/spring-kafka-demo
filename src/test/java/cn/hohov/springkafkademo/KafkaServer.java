package cn.hohov.springkafkademo;

import cn.hohov.springkafkademo.producer.SenderConfig;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;

@SpringBootTest(classes = KafkaServer.class)
@EmbeddedKafka(topics = {SenderConfig.TOPIC_1, SenderConfig.TOPIC_2, SenderConfig.TOPIC_3}, count = 3, ports = {50001, 50002, 50003}, partitions = 1)
public class KafkaServer {

    @Test
    public void startKafka() {
        for (; ; ) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
                break;
            }
        }
    }
}
