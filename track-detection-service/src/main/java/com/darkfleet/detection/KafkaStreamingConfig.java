package com.darkfleet.detection;

import com.darkfleet.common.kafka.DarkFleetTopics;
import com.darkfleet.common.kafka.JsonSerde;
import com.darkfleet.common.model.CoverageCellHour;
import com.darkfleet.common.model.DetectionEvent;
import com.darkfleet.common.model.DetectionRuleConfig;
import com.darkfleet.common.model.EncounterCandidate;
import com.darkfleet.common.model.NormalizedAisMessage;
import com.darkfleet.common.model.VesselTrack;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Configuration
@EnableKafka
public class KafkaStreamingConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaStreamingConfig.class);

    @Value("${spring.kafka.bootstrap-servers:localhost:9092}")
    private String bootstrapServers;

    @Value("${darkfleet.kafka-streams.application-id:darkfleet-track-detection-streams}")
    private String applicationId;

    private final TrackAndDetectionEngine engine;

    public KafkaStreamingConfig(TrackAndDetectionEngine engine) {
        this.engine = engine;
    }

    /**
     * Builds the Kafka Streams topology:
     * 1. Ingests ais.raw partitioned by MMSI -> reconstructs tracks, updates coverage, emits gap/loitering detections.
     * 2. Re-keys slow-moving cargo/tanker reports by H3 grid cell -> ais.candidates.encounter.
     * 3. Consumes ais.candidates.encounter partitioned by H3 cell -> correlates pairwise STS encounters -> ais.detections.
     */
    public Topology buildStreamsTopology(StreamsBuilder builder) {
        JsonSerde<NormalizedAisMessage> rawSerde = new JsonSerde<>(NormalizedAisMessage.class);
        JsonSerde<VesselTrack> trackSerde = new JsonSerde<>(VesselTrack.class);
        JsonSerde<CoverageCellHour> coverageSerde = new JsonSerde<>(CoverageCellHour.class);
        JsonSerde<EncounterCandidate> candidateSerde = new JsonSerde<>(EncounterCandidate.class);
        JsonSerde<DetectionEvent> detectionSerde = new JsonSerde<>(DetectionEvent.class);
        JsonSerde<DetectionRuleConfig> ruleSerde = new JsonSerde<>(DetectionRuleConfig.class);

        KStream<String, NormalizedAisMessage> rawStream = builder.stream(
                DarkFleetTopics.AIS_RAW,
                Consumed.with(Serdes.String(), rawSerde)
        );

        KStream<String, TrackAndDetectionEngine.ProcessingOutcome> outcomes = rawStream
                .mapValues(engine::processMessage);

        // Publish updated vessel tracks partitioned by MMSI
        outcomes.filter((mmsi, outcome) -> outcome != null && outcome.updatedTrack() != null)
                .mapValues(TrackAndDetectionEngine.ProcessingOutcome::updatedTrack)
                .to(DarkFleetTopics.AIS_TRACKS, Produced.with(Serdes.String(), trackSerde));

        // Publish updated cell coverage partitioned by h3Cell
        outcomes.filter((mmsi, outcome) -> outcome != null && outcome.updatedCoverage() != null)
                .map((mmsi, outcome) -> KeyValue.pair(
                        outcome.updatedCoverage().h3Cell(),
                        outcome.updatedCoverage()
                ))
                .to(DarkFleetTopics.AIS_COVERAGE, Produced.with(Serdes.String(), coverageSerde));

        // Publish MMSI-partitioned detections (unexplained gaps, loitering, draught changes, low-confidence signals)
        outcomes.filter((mmsi, outcome) -> outcome != null && !outcome.newDetections().isEmpty())
                .flatMapValues(TrackAndDetectionEngine.ProcessingOutcome::newDetections)
                .to(DarkFleetTopics.AIS_DETECTIONS, Produced.with(Serdes.String(), detectionSerde));

        // Re-key slow-moving cargo/tanker candidates by H3 grid cell so ships on different MMSI partitions meet on the same cell partition
        outcomes.filter((mmsi, outcome) -> outcome != null && !outcome.emittedEncounterCandidates().isEmpty())
                .flatMap((mmsi, outcome) -> outcome.emittedEncounterCandidates().stream()
                        .map(cand -> KeyValue.pair(cand.h3Cell(), cand))
                        .toList())
                .to(DarkFleetTopics.AIS_ENCOUNTER_CANDIDATES, Produced.with(Serdes.String(), candidateSerde));

        // Consume H3-partitioned encounter candidates and emit potential STS encounter detections
        builder.stream(DarkFleetTopics.AIS_ENCOUNTER_CANDIDATES, Consumed.with(Serdes.String(), candidateSerde))
                .flatMap((h3Cell, candidate) -> {
                    List<DetectionEvent> encounterEvents = engine.processEncounterCandidate(candidate);
                    return encounterEvents.stream()
                            .map(ev -> KeyValue.pair(ev.mmsi(), ev))
                            .toList();
                })
                .to(DarkFleetTopics.AIS_DETECTIONS, Produced.with(Serdes.String(), detectionSerde));

        // Dynamic rule updates stream
        builder.stream(DarkFleetTopics.DARKFLEET_RULES, Consumed.with(Serdes.String(), ruleSerde))
                .foreach((key, ruleConfig) -> {
                    log.info("Applying updated detection rules version {} from Kafka stream", ruleConfig.ruleVersion());
                    engine.updateRules(ruleConfig);
                });

        return builder.build();
    }

    @Bean
    public KafkaTemplate<String, Object> genericKafkaTemplate() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        JsonSerde<Object> objectSerde = new JsonSerde<>(Object.class);
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(props, new StringSerializer(), objectSerde.serializer()));
    }

    @Bean
    public NewTopic tracksTopic() {
        return TopicBuilder.name(DarkFleetTopics.AIS_TRACKS).partitions(6).replicas(1).build();
    }

    @Bean
    public NewTopic encounterCandidatesTopic() {
        return TopicBuilder.name(DarkFleetTopics.AIS_ENCOUNTER_CANDIDATES).partitions(6).replicas(1).build();
    }

    @Bean
    public NewTopic detectionsTopic() {
        return TopicBuilder.name(DarkFleetTopics.AIS_DETECTIONS).partitions(6).replicas(1).build();
    }

    @Bean
    public NewTopic coverageTopic() {
        return TopicBuilder.name(DarkFleetTopics.AIS_COVERAGE).partitions(6).replicas(1).build();
    }

    @Bean
    public NewTopic rulesTopic() {
        return TopicBuilder.name(DarkFleetTopics.DARKFLEET_RULES).partitions(1).replicas(1).build();
    }

    @Configuration
    @EnableKafkaStreams
    @ConditionalOnProperty(name = "darkfleet.kafka-streams.enabled", havingValue = "true", matchIfMissing = false)
    public static class ActiveKafkaStreamsConfiguration {

        private final KafkaStreamingConfig parent;

        public ActiveKafkaStreamsConfiguration(KafkaStreamingConfig parent) {
            this.parent = parent;
        }

        @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
        public KafkaStreamsConfiguration kStreamsConfigs() {
            Map<String, Object> props = new HashMap<>();
            props.put(StreamsConfig.APPLICATION_ID_CONFIG, parent.applicationId);
            props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, parent.bootstrapServers);
            props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
            return new KafkaStreamsConfiguration(props);
        }

        @Bean
        public Topology darkFleetTopology(StreamsBuilder streamsBuilder) {
            return parent.buildStreamsTopology(streamsBuilder);
        }
    }

    @KafkaListener(
            topics = DarkFleetTopics.AIS_RAW,
            groupId = "darkfleet-track-detection-direct",
            autoStartup = "${darkfleet.kafka-listener.enabled:false}"
    )
    public void onRawAisPayload(String rawJson) {
        try {
            NormalizedAisMessage msg = JsonSerde.mapper().readValue(rawJson, NormalizedAisMessage.class);
            engine.processMessage(msg);
        } catch (Exception e) {
            log.warn("Failed to process raw AIS message from Kafka listener: {}", e.getMessage());
        }
    }
}
