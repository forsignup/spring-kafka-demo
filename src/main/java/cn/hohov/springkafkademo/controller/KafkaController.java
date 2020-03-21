package cn.hohov.springkafkademo.controller;

import cn.hohov.springkafkademo.producer.Sender;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import static cn.hohov.springkafkademo.producer.SenderConfig.*;

@RestController
@RequestMapping("/kafka")
public class KafkaController {

    @Autowired
    private Sender sender;

    @GetMapping("/one")
    public String sendOne() {
        sender.sendOne(TOPIC_1, "hello, one");
        return "ok";
    }

    @GetMapping("/two")
    public String sendTwo() {
        sender.sendTwo(TOPIC_2, "hello, two");
        return "ok";
    }

    @GetMapping("/three")
    public String sendThree() {
        sender.sendThree(TOPIC_3, "hello, three");
        return "ok";
    }
}
