package com.example.kafka.demo;

import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.listener.ConsumerAwareListenerErrorHandler;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.MessageHeaders;

@SpringBootApplication
public class SampleKafkaApplication {

    public static void main(String[] args) {
        SpringApplication.run(SampleKafkaApplication.class, args);
    }

	private ListenerExecutionFailedException listen3Exception;

    @Bean
    public ApplicationRunner runner(Producer producer) {
        return args -> {
                    producer.send(new SampleMessage(1, "A simple test message 1"));
                    producer.send(new SampleMessage(2, "A simple test message 2"));
                    producer.send(new SampleMessage(3, "A simple test message 3"));
                    producer.send(new SampleMessage(4, "A simple test message 4"));
        };
    }
    
    @Bean
    public ConsumerAwareListenerErrorHandler listen3ErrorHandler() {
        return (m, e, c) -> {
            this.listen3Exception = e;
            MessageHeaders headers = m.getHeaders();
            c.seek(new org.apache.kafka.common.TopicPartition(
                    headers.get(KafkaHeaders.RECEIVED_TOPIC, String.class),
                    headers.get(KafkaHeaders.RECEIVED_PARTITION_ID, Integer.class)),
                    headers.get(KafkaHeaders.OFFSET, Long.class));
            return null;
        };
    }

}
