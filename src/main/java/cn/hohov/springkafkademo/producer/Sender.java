package cn.hohov.springkafkademo.producer;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;

@Data
@Slf4j
public class Sender<T, R> {

    public Sender(KafkaTemplate<T, R> kafkaTemplateOne, KafkaTemplate<T, R> kafkaTemplateTwo, KafkaTemplate<T, R> kafkaTemplateThree) {
        this.kafkaTemplateOne = kafkaTemplateOne;
        this.kafkaTemplateTwo = kafkaTemplateTwo;
        this.kafkaTemplateThree = kafkaTemplateThree;
    }

    private final KafkaTemplate<T, R> kafkaTemplateOne;
    private final KafkaTemplate<T, R> kafkaTemplateTwo;
    private final KafkaTemplate<T, R> kafkaTemplateThree;


    public void sendOne(String topic, R playLoad) {
        sendMsg(kafkaTemplateOne, topic, playLoad);
    }

    public void sendTwo(String topic, R playLoad) {
        sendMsg(kafkaTemplateTwo, topic, playLoad);
    }

    public void sendThree(String topic, R playLoad) {
        sendMsg(kafkaTemplateThree, topic, playLoad);
    }

    private void sendMsg(KafkaTemplate<T, R> kafkaTemplate, String topic, R playLoad) {
        log.info("发送消息, topic: <{}>, msg: <{}>", topic, playLoad);
        kafkaTemplate.send(topic, playLoad);
    }

}
