package com.darkfleet.intelligence;

import com.darkfleet.common.kafka.JsonSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import com.darkfleet.security.ApiKeyAuthenticationFilter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.function.Predicate;

import static org.awaitility.Awaitility.await;

/**
 * Runs the service against a real PostGIS and a real Kafka, the same images as in Compose. The
 * containers are started once and shared by every test class (one per JVM). Tests share the
 * database, so each one uses its own MMSIs and ids. Scheduled retention is off (tests run it with
 * their own clock); the sanction list is a small sample imported on request.
 */
@SpringBootTest
@AutoConfigureMockMvc
@TestPropertySource(properties = {
        "darkfleet.security.api-clients[0].name=admin",
        "darkfleet.security.api-clients[0].key=" + IntegrationTest.ADMIN_KEY,
        "darkfleet.security.api-clients[0].roles=ADMIN,ANALYST",
        "darkfleet.security.api-clients[1].name=analyst",
        "darkfleet.security.api-clients[1].key=" + IntegrationTest.ANALYST_KEY,
        "darkfleet.security.api-clients[1].roles=ANALYST",
        "darkfleet.security.api-clients[2].name=mcp",
        "darkfleet.security.api-clients[2].key=" + IntegrationTest.SERVICE_KEY,
        "darkfleet.security.api-clients[2].roles=SERVICE",
        "darkfleet.intelligence.retention.enabled=false",
        "darkfleet.intelligence.sanctions.url=classpath:sanctions/maritime-sample.csv",
        "darkfleet.intelligence.sanctions.first-check=PT24H"
})
public abstract class IntegrationTest {

    public static final String ADMIN_KEY = "test-admin-key";
    public static final String ANALYST_KEY = "test-analyst-key";
    public static final String SERVICE_KEY = "test-service-key";

    @Autowired
    protected MockMvc mvc;

    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer(
            DockerImageName.parse("postgis/postgis:18-3.7").asCompatibleSubstituteFor("postgres"))
            .withDatabaseName("darkfleet")
            .withUsername("darkfleet");

    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.4.0");

    private static final KafkaProducer<String, byte[]> PRODUCER;

    static {
        Startables.deepStart(POSTGRES, KAFKA).join();
        PRODUCER = new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ProducerConfig.ACKS_CONFIG, "all"),
                new StringSerializer(), new ByteArraySerializer());
    }

    @Autowired
    protected JdbcTemplate jdbc;

    @DynamicPropertySource
    static void containers(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    }

    /** Publishes a message as Track &amp; Detection would: JSON, keyed. */
    protected static void send(String topic, String key, Object message) {
        sendRaw(topic, key, JsonSerde.mapper().writeValueAsBytes(message));
    }

    protected static void sendRaw(String topic, String key, byte[] value) {
        try {
            PRODUCER.send(new ProducerRecord<>(topic, key, value)).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        } catch (ExecutionException e) {
            throw new IllegalStateException(e.getCause());
        }
    }

    protected static MockHttpServletRequestBuilder get(String key, String path, Object... vars) {
        return withKey(MockMvcRequestBuilders.get(path, vars), key);
    }

    protected static MockHttpServletRequestBuilder post(String key, String path, Object body, Object... vars) {
        MockHttpServletRequestBuilder request = withKey(MockMvcRequestBuilders.post(path, vars), key);
        return body == null ? request
                : request.contentType(MediaType.APPLICATION_JSON).content(JsonSerde.mapper().writeValueAsString(body));
    }

    private static MockHttpServletRequestBuilder withKey(MockHttpServletRequestBuilder request, String key) {
        return key == null ? request : request.header(ApiKeyAuthenticationFilter.HEADER, key);
    }

    /** Reads a topic from the start until a record matches, or fails after 30 s. */
    protected static ConsumerRecord<String, String> awaitRecord(String topic, Predicate<ConsumerRecord<String, String>> match) {
        try (var consumer = new KafkaConsumer<>(Map.<String, Object>of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "test-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"),
                new StringDeserializer(), new StringDeserializer())) {
            consumer.subscribe(List.of(topic));
            long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            while (System.nanoTime() < deadline) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(500))) {
                    if (match.test(record)) {
                        return record;
                    }
                }
            }
        }
        throw new AssertionError("No matching record on " + topic);
    }

    /** Waits until the query returns the expected count. */
    protected void awaitCount(long expected, String sql, Object... args) {
        await().atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofMillis(200))
                .until(() -> jdbc.queryForObject(sql, Long.class, args) == expected);
    }
}
