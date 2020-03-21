package cn.hohov.springkafkademo.producer;

import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.HashMap;

@Configuration
public class SenderConfig {

    public static final String TOPIC_1 = "topic_1";
    public static final String TOPIC_2 = "topic_2";
    public static final String TOPIC_3 = "topic_3";

    @Bean("kafka-one")
    public KafkaTemplate<String, String> getKafkaOne(@Value("${kafka.one.bootstrap-servers}") String bootstrapServers,
                                                     @Value("${kafka.one.consumer.group-id}") String groupId,
                                                     @Value("${kafka.one.consumer.enable-auto-commit}") String enableAutoCommit) {

        return getKafkaTemplate(bootstrapServers);

    }

    @Bean("kafka-two")
    public KafkaTemplate<String, String> getKafkaTwo(@Value("${kafka.two.bootstrap-servers}") String bootstrapServers) {

        return getKafkaTemplate(bootstrapServers);

    }

    @Bean("kafka-three")
    public KafkaTemplate<String, String> getKafkaThree(@Value("${kafka.three.bootstrap-servers}") String bootstrapServers) {

        return getKafkaTemplate(bootstrapServers);

    }

    private KafkaTemplate<String, String> getKafkaTemplate(String bootstrapServers) {
        HashMap<String, Object> config = Maps.newHashMap();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ProducerConfig.RETRIES_CONFIG, 0);
        config.put(ProducerConfig.ACKS_CONFIG, "1");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        DefaultKafkaProducerFactory<String, String> producerFactory = new DefaultKafkaProducerFactory<>(config);
        return new KafkaTemplate<>(producerFactory);
    }

    @Bean
    public Sender<String, String> getSender(@Qualifier("kafka-one")KafkaTemplate<String, String> one,
                            @Qualifier("kafka-two")KafkaTemplate<String, String> two,
                            @Qualifier("kafka-three")KafkaTemplate<String, String> three) {
        return new Sender<>(one, two, three);
    }
}
