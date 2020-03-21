package cn.hohov.springkafkademo.consumer;

import cn.hohov.springkafkademo.producer.SenderConfig;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;

@Data
@Slf4j
public class Receiver {

    @KafkaListener(topics = SenderConfig.TOPIC_1, containerFactory = "consumerOne")
    public void handle(String msg) {
        log.info("consumer01收到消息: <{}>", msg);
    }

    @KafkaListener(topics = SenderConfig.TOPIC_2, containerFactory = "consumerTwo")
    public void handle02(String msg) {
        log.info("consumer02收到消息: <{}>", msg);
    }

    @KafkaListener(topics = SenderConfig.TOPIC_3, containerFactory = "consumerThree")
    public void handle03(String msg) {
        log.info("consumer03收到消息: <{}>", msg);
    }
}
