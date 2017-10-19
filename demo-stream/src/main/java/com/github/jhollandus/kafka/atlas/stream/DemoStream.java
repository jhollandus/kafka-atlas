package com.github.jhollandus.kafka.atlas.stream;

import com.google.common.base.CaseFormat;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.atlas.AtlasBaseClient;
import org.apache.atlas.AtlasClient;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.MapConfiguration;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DemoStream {
    private static final Logger log = LoggerFactory.getLogger(DemoStream.class);

    private final Properties kafkaprops;
    private final DemoStreamConfig atlasCfg;

    public static void main(String[] args) throws Exception {
        String inputTopics = System.getProperty("input.topics", "cdc-db-demo-accounts,cdc-db-demo-users");
        String appId = System.getProperty("application.id", "account-proc");

        System.setProperty("atlas.conf", "./");

        DemoStreamConfig atlasStreamConfig = new DemoStreamConfig()
                .withAppId(appId)
                .withAtlasUrl("http://localhost:21000")
                .withInputTopics(Arrays.asList(inputTopics.split(",")))
                .withOutputTopics(Stream.of(inputTopics.split(",")).map(t -> t + "-" + appId).collect(Collectors.toList()))
                .withKafkaCluster("demo")
                .withKafkaClusterLocation("localhost");

        DemoStream demoStream = new DemoStream(atlasStreamConfig);
        KafkaStreams streams = demoStream.createStream();

        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("atlas-hook-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            log.debug("Starting stream {}.", appId);
            streams.start();
            demoStream.publishAtlasMetadata();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }

    public DemoStream(DemoStreamConfig atlasCfg) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, atlasCfg.getAppId());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");
        props.put("schema.registry.url", "http://localhost:8081");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.ByteArraySerde.class.getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class.getName());

        kafkaprops = props;
        this.atlasCfg = atlasCfg;
    }

    public KafkaStreams createStream() {

        KStreamBuilder kstream = new KStreamBuilder();
        GenericAvroSerde avroSerde = new GenericAvroSerde();
        avroSerde.configure(Maps.fromProperties(kafkaprops), false);

        for(String input: atlasCfg.getInputTopics()) {
            KStream<byte[], GenericRecord> stream = kstream.stream(input);
            stream.mapValues(v -> {
                log.debug("Received messgage {}", v);
                SchemaBuilder.RecordBuilder<Schema> recordBulder = SchemaBuilder.record(
                        CaseFormat.LOWER_HYPHEN.to(CaseFormat.LOWER_CAMEL, input + "-" + atlasCfg.getAppId()));
                SchemaBuilder.FieldAssembler<Schema> fieldAssembler = recordBulder.fields();
                v.getSchema().getFields().forEach(f ->
                        fieldAssembler.name(f.name()).type(f.schema()));
                Schema updSchema = fieldAssembler.endRecord();

                GenericRecordBuilder recordBuilder = new GenericRecordBuilder(updSchema);
                v.getSchema().getFields().forEach(f -> {
                    if (f.schema().getType() == Schema.Type.STRING) {
                        if (v.get(f.name()) != null) {
                            recordBuilder.set(f.name(), v.get(f.name()).toString().toUpperCase());
                        }
                    }
                });

                return (GenericRecord) recordBuilder.build();
            }).to(Serdes.ByteArray(), avroSerde, input + "-" + atlasCfg.getAppId());
        }

        return new KafkaStreams(kstream, kafkaprops);
    }

    protected void publishAtlasMetadata() {
        AtlasClient atlasClient = new AtlasClient(new String[]{atlasCfg.getAtlasUrl()}, new String[]{"admin", "admin"});

        boolean retry = true;
        while(retry) {
            try {
                updateMetadata(atlasClient);
                retry = false;
            } catch (AtlasServiceException e) {
                log.error("", e);
                try {
                    Thread.sleep(5000);
                } catch(InterruptedException e2) {
                    //no-op
                }
            }
        }
    }

    private void updateMetadata(AtlasClient atlasClient) throws AtlasServiceException {

        List<Referenceable> inputRefs = fetchTopics(atlasClient, atlasCfg.getInputTopics());

        List<Referenceable> outputRefs = fetchTopics(atlasClient, atlasCfg.getOutputTopics());

        String name = atlasCfg.getAppId();
        String id = atlasCfg.getKafkaCluster() + "." + name;
        Referenceable streamProcRef = new Referenceable("kafka_streams_process");
        streamProcRef.set("name", name);
        streamProcRef.set(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, id);
        streamProcRef.set("applicationId", name);
        streamProcRef.set("inputs", inputRefs);
        streamProcRef.set("outputs", outputRefs);

        try {
            atlasClient.createEntity(streamProcRef);
        } catch(AtlasServiceException e) {
            if(e.getStatus().getStatusCode() == 409) {
                atlasClient.updateEntity("kafka_streams_process", AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, id, streamProcRef);
            }
        }
    }

    private List<Referenceable> fetchTopics(AtlasClient atlasClient, List<String> topicNames) throws AtlasServiceException {

        List<Referenceable> topics = new LinkedList<>();
        for (String topic : topicNames) {
            Referenceable topicRef =
                    atlasClient.getEntity(
                            "kafka_topic",
                            AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME,
                            atlasCfg.getKafkaCluster() + "." + topic);
            log.debug("Found atlas topic {}", topicRef.getId().id);
            topics.add(topicRef);
        }

        return topics;
    }
}
