package com.hari.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.hari.dto.AssignRoleEvent;
import org.apache.kafka.clients.admin.AdminClientConfig;
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
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
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
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        return new KafkaAdmin(config);
    }

    public static final String ROLE_ASSIGNMENT_DLT = ROLE_ASSIGNMENT_TOPIC + ".DLT";

    @Bean
    public NewTopic roleAssignmentTopic() {
        // NOTE: replicas must never exceed the number of brokers in the
        // cluster (docker-compose runs a single Kafka broker), otherwise
        // topic creation fails with InvalidReplicationFactorException and
        // the whole app fails to start.
        return TopicBuilder.name(ROLE_ASSIGNMENT_TOPIC)
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic roleAssignmentDeadLetterTopic() {
        // Where permanently-failed events (NonRetryableAssignmentException,
        // poison-pill deserialization failures) are routed for audit/replay
        // instead of being silently dropped.
        return TopicBuilder.name(ROLE_ASSIGNMENT_DLT)
                .partitions(3)
                .replicas(1)
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
        // Strong delivery returns ack only after sync to all replications so we don't lose the request before it hits the queue.
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        config.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        // IMPORTANT: retries=MAX_VALUE is meaningless on its own — the
        // producer gives up once delivery.timeout.ms elapses (default
        // 120_000ms = 2 minutes), regardless of retry count. Without raising
        // this, a broker outage longer than 2 minutes would still fail the
        // send and the "sendResult.whenComplete" error branch would fire,
        // even though the log comment above claims we never lose the event.
        config.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, Integer.MAX_VALUE);
        config.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30_000);
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
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // Only commit the offset once the listener completes successfully.
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
        // The blocking retry loop below (FixedBackOff, UNLIMITED_ATTEMPTS)
        // runs synchronously on the poll thread. If a DB outage lasts longer
        // than max.poll.interval.ms (default 300_000ms/5min), the broker
        // evicts this consumer from the group -> rebalance -> the partition
        // is reassigned (possibly to the same instance) and processing
        // restarts, potentially in a repeating rebalance storm for the
        // whole duration of the outage. Raise this well above any realistic
        // outage window so retries can safely keep blocking without churn.
        config.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, Integer.MAX_VALUE);

        JsonDeserializer<AssignRoleEvent> jsonDeserializer =
                new JsonDeserializer<>(AssignRoleEvent.class, kafkaObjectMapper());
        jsonDeserializer.addTrustedPackages("com.hari.dto");
        // Ignore any type-info header set by the producer (we disabled it there
        // anyway) and always deserialize to AssignRoleEvent regardless of headers.
        jsonDeserializer.ignoreTypeHeaders();
        // Wrap in ErrorHandlingDeserializer: without this, a malformed
        // ("poison pill") record throws inside poll() itself, before any
        // listener/error-handler logic runs — this can kill the container
        // or spin forever on the same unreadable record. The wrapper
        // catches the deserialization failure and hands a
        // DeserializationException to the listener container's error
        // handler/recoverer instead, so it gets routed to the DLT like any
        // other permanent failure.
        ErrorHandlingDeserializer<AssignRoleEvent> deserializer = new ErrorHandlingDeserializer<>(jsonDeserializer);

        return new DefaultKafkaConsumerFactory<>(
                config,
                new StringDeserializer(),
                deserializer
        );
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, AssignRoleEvent> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, AssignRoleEvent> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        // Match the topic's partition count so all 3 partitions can be
        // consumed in parallel instead of a single thread serializing them.
        factory.setConcurrency(3);

        // Recover permanently-failed records (business rule failures via
        // NonRetryableAssignmentException, and poison-pill deserialization
        // failures via ErrorHandlingDeserializer) by publishing them to the
        // "<topic>.DLT" topic for audit/manual replay, instead of silently
        // dropping them.
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate());

        // Retry indefinitely with a fixed delay between attempts. Because the
        // same message keeps being retried until it succeeds, no data is lost
        // and the offset only advances on success.
        DefaultErrorHandler errorHandler =
                new DefaultErrorHandler(recoverer, new FixedBackOff(RETRY_INTERVAL_MS, FixedBackOff.UNLIMITED_ATTEMPTS));
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








