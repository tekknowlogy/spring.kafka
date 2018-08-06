package com.example.kafka.demo;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.config.ContainerProperties;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.requestreply.RequestReplyFuture;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.util.concurrent.ListenableFutureCallback;

@SpringBootApplication
public class KRequestingApplication {
	
    public static void main(String[] args) {
        SpringApplication.run(KRequestingApplication.class, args).close();
    }
    
    @Value("${spring.kafka.bootstrap-servers}")
    public String bootstrapServers;

    @Bean
    public ApplicationRunner runner(ReplyingKafkaTemplate<String, String, String> template) {
        return args -> {
            ProducerRecord<String, String> record = new ProducerRecord<>("kRequests", "messageForReply");
            record.headers().add(new RecordHeader(KafkaHeaders.REPLY_TOPIC, "kReplies".getBytes()));
            RequestReplyFuture<String, String, String> replyFuture = template.sendAndReceive(record);
            replyFuture.getSendFuture().addCallback(new ListenableFutureCallback<SendResult<String, String>>() {

                @Override
                public void onSuccess(SendResult<String, String> result) {
                	System.out.println("callback sending : "+result.getRecordMetadata());
                }

                @Override
                public void onFailure(Throwable ex) {
                	ex.printStackTrace();
                }

            });
            replyFuture.addCallback(new ListenableFutureCallback<ConsumerRecord<String, String>>() {

                @Override
                public void onSuccess(ConsumerRecord<String, String> result) {
                	System.out.println("callback receiving : "+result.value());
                }

                @Override
                public void onFailure(Throwable ex) {
                	ex.printStackTrace();
                }

            });
            Thread.sleep(100000);
        };
    }

    @Bean
    public ReplyingKafkaTemplate<String, String, String> kafkaTemplate(
            ProducerFactory<String, String> pf,
            KafkaMessageListenerContainer<String, String> replyContainer) {
        return new ReplyingKafkaTemplate<>(pf, replyContainer);
    }

    @Bean
    public KafkaMessageListenerContainer<String, String> replyContainer(
            ConsumerFactory<String, String> cf) {
        ContainerProperties containerProperties = new ContainerProperties("kReplies");
        return new KafkaMessageListenerContainer<>(cf, containerProperties);
    }
    
    @Bean
    public Map<String, Object> consumerConfigs() {
        Map<String, Object> props = new HashMap<>();
        // list of host:port pairs used for establishing the initial connections to the Kakfa cluster
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        // allows a pool of processes to divide the work of consuming and processing records
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "bot");

        return props;
    }

    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(consumerConfigs());
    }
    
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String>
    kafkaListenerContainerFactory() {
      ConcurrentKafkaListenerContainerFactory<String, String> factory =
          new ConcurrentKafkaListenerContainerFactory<>();
      factory.setConsumerFactory(consumerFactory());
      factory.setReplyTemplate(kafkaTemplate());
      return factory;
    }
    
    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }
    
    @Bean
    public ProducerFactory<String, String> producerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        // set more properties
        return new DefaultKafkaProducerFactory<>(props);
    }
    
    @KafkaListener(topics = "kRequests")
    @SendTo("kReplies")
    public String listen(String in) {
        System.out.println("Server received: " + in);
        return in.toUpperCase();
    }


}