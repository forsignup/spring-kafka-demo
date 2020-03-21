package cn.hohov.springkafkademo.consumer;

import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import java.util.HashMap;

@Configuration
public class ReceiverConfig {

    @Bean
    public Receiver getReceiver() {
        return new Receiver();
    }

    @Bean("consumerOne")
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactoryOne(@Value("${kafka.one.bootstrap-servers}") String bootstrapServers,
                                                                                                 @Value("${kafka.one.consumer.group-id}") String groupId,
                                                                                                 @Value("${kafka.one.consumer.enable-auto-commit}") String enableAutoCommit) {
        return getContainerFactory(bootstrapServers, groupId, enableAutoCommit);
    }

    @Bean("consumerTwo")
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactoryTwo(@Value("${kafka.two.bootstrap-servers}") String bootstrapServers,
                                                                                                 @Value("${kafka.two.consumer.group-id}") String groupId,
                                                                                                 @Value("${kafka.two.consumer.enable-auto-commit}") String enableAutoCommit) {
        return getContainerFactory(bootstrapServers, groupId, enableAutoCommit);
    }

    @Bean("consumerThree")
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactoryThree(@Value("${kafka.three.bootstrap-servers}") String bootstrapServers,
                                                                                                 @Value("${kafka.three.consumer.group-id}") String groupId,
                                                                                                 @Value("${kafka.three.consumer.enable-auto-commit}") String enableAutoCommit) {
        return getContainerFactory(bootstrapServers, groupId, enableAutoCommit);
    }

    private ConcurrentKafkaListenerContainerFactory<String, String> getContainerFactory(String bootstrapServers,String groupId, String enableAutoCommit) {
        ConcurrentKafkaListenerContainerFactory<String, String> containerFactory = new ConcurrentKafkaListenerContainerFactory<>();

        HashMap<String, Object> config = Maps.newHashMap();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, enableAutoCommit);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        DefaultKafkaConsumerFactory<String, String> consumerFactory = new DefaultKafkaConsumerFactory<>(config);
        containerFactory.setConsumerFactory(consumerFactory);
        return containerFactory;
    }


}
