package com.hari.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.hari.dto.AssignRoleEvent;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafka
public class KafkaConfig {

    public static final String ROLE_ASSIGNMENT_TOPIC = "employee.role-assignment";

    private static final long RETRY_INTERVAL_MS = 5_000L;

    private static String bootstrapServers() {
        String val = System.getenv("KAFKA_BOOTSTRAP_SERVERS");
        return (val != null && !val.isBlank()) ? val : "kafka:9092";
    }

    @Bean
    public KafkaAdmin kafkaAdmin() {
        Map<String, Object> config = new HashMap<>();
        config.put(org.apache.kafka.clients.admin.AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        return new KafkaAdmin(config);
    }

    @Bean
    public NewTopic roleAssignmentTopic() {
        return TopicBuilder.name(ROLE_ASSIGNMENT_TOPIC)
                .partitions(3)
                .replicas(2)
                .build();
    }

    //Producer
    private static ObjectMapper kafkaObjectMapper() {
        return new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    @Bean
    public ProducerFactory<String, Object> producerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        // Strong delivery guarantees so we don't lose the request before it hits the queue.
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        config.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        JsonSerializer<Object> serializer = new JsonSerializer<>(kafkaObjectMapper());
        serializer.setAddTypeInfo(false);
        return new DefaultKafkaProducerFactory<>(config, new StringSerializer(), serializer);
    }

    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    // ----------------- Consumer -----------------

    @Bean
    public ConsumerFactory<String, AssignRoleEvent> consumerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "employee-role-assignment-group");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // Only commit the offset once the listener completes successfully.
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);

        JsonDeserializer<AssignRoleEvent> deserializer =
                new JsonDeserializer<>(AssignRoleEvent.class, kafkaObjectMapper());
        deserializer.addTrustedPackages("com.hari.dto");
        // Ignore any type-info header set by the producer (we disabled it there
        // anyway) and always deserialize to AssignRoleEvent regardless of headers.
        deserializer.ignoreTypeHeaders();

        return new DefaultKafkaConsumerFactory<>(
                config,
                new StringDeserializer(),
                deserializer
        );
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, AssignRoleEvent> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, AssignRoleEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());

        // Retry indefinitely with a fixed delay between attempts. Because the
        // same message keeps being retried until it succeeds, no data is lost
        // and the offset only advances on success.
        DefaultErrorHandler errorHandler =
                new DefaultErrorHandler(new FixedBackOff(RETRY_INTERVAL_MS, FixedBackOff.UNLIMITED_ATTEMPTS));
        // Permanent/business errors (bad request, not found, etc.) should not
        // be retried forever - only transient failures (DB down, network
        // blips, etc.) are worth an indefinite retry loop.
        // Only genuine, permanent business-rule failures should stop retrying.
        // AppException itself is NOT excluded here because the repository
        // layer also wraps transient technical failures (e.g. DB connection
        // refused) as AppException(500) — those must keep retrying.
        errorHandler.addNotRetryableExceptions(com.hari.service.NonRetryableAssignmentException.class);
        factory.setCommonErrorHandler(errorHandler);

        return factory;
    }
}








