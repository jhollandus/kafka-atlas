package com.github.jhollandus.kafka.atlas;

import com.google.common.collect.ImmutableSet;
import io.confluent.kafka.schemaregistry.storage.DeleteSubjectValue;
import io.confluent.kafka.schemaregistry.storage.SchemaRegistryKey;
import io.confluent.kafka.schemaregistry.storage.SchemaRegistryValue;
import io.confluent.kafka.schemaregistry.storage.SchemaValue;
import io.confluent.kafka.schemaregistry.storage.serialization.SchemaRegistrySerializer;
import io.confluent.kafka.serializers.KafkaJsonDeserializer;
import io.confluent.kafka.serializers.KafkaJsonSerializer;
import org.apache.atlas.notification.AbstractNotification;
import org.apache.atlas.notification.hook.HookNotification;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.json.InstanceSerialization;
import org.apache.avro.Schema;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.codehaus.jettison.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class KafkaAtlasHook {
    private static final Logger log = LoggerFactory.getLogger(KafkaAtlasHook.class);

    private final SchemaRegistrySerializer serializer = new SchemaRegistrySerializer();
    private final AvroEntityProvider avroEntityProvider;
    private final KafkaEntityProvider kafkaEntityProvider;

    public static void main(String[] args) throws Exception {
        String schemasTopic = System.getProperty("topic.schemas", "_schemas");
        String atlasTopic = System.getProperty("topic.atlas.hook", "ATLAS_HOOK");
        String clusterName = System.getProperty("cluster.name", "demo");
        String clusterLocation = System.getProperty("cluster.location", "localhost");

        KafkaStreams streams = new KafkaAtlasHook(clusterName, clusterLocation)
                    .createStream(schemasTopic, atlasTopic);

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
            log.debug("Starting stream.");
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }

    public KafkaAtlasHook(String clusterName, String clusterLocation) {
        this.kafkaEntityProvider = new KafkaEntityProvider(clusterName, clusterLocation);
        this.avroEntityProvider = new AvroEntityProvider();
    }

    public KafkaStreams createStream(String schemasTopic, String atlasTopic) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "atlas-integrator");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass().getName());

        KStreamBuilder builder = new KStreamBuilder();

        KStream<byte[], byte[]> schemaStream = builder.stream(TopologyBuilder.AutoOffsetReset.EARLIEST, schemasTopic);


        KafkaJsonSerializer<HookNotification.HookNotificationMessage> atlasSerializer = new KafkaJsonSerializer<>();
        atlasSerializer.configure(Collections.emptyMap(), false);

        KafkaJsonDeserializer<HookNotification.HookNotificationMessage> atlasDeserializer = new KafkaJsonDeserializer<>();
        atlasDeserializer.configure(Collections.emptyMap(), false);

        //need to store all created entities so we can update them correctly
        schemaStream.flatMap((k, v) ->
                    deserialize(k, v)
                                .map(Collections::singletonList)
                                .orElseGet(Collections::emptyList))
        .map((k, v) -> new KeyValue<>(k.getMagicByte(), toAtlasFormat(k, v)))
                .filter((k, v) -> !Objects.isNull(k) && !Objects.isNull(v))
                .to(Serdes.Integer(), Serdes.String(), atlasTopic);



        return new KafkaStreams(builder, props);
    }

    private Optional<KeyValue<SchemaRegistryKey, SchemaRegistryValue>> deserialize(byte[] rawKey, byte[] rawValue) {
        try {
            SchemaRegistryKey key = serializer.deserializeKey(rawKey);
            switch (key.getKeyType()) {
                case NOOP:
                    log.debug("NOOP key found, ignoring.");
                    break;
                case CONFIG:
                    log.debug("Config update found, ignoring.");
                    break;
                case DELETE_SUBJECT: case SCHEMA:
                    return Optional.of(KeyValue.pair(key, serializer.deserializeValue(key, rawValue)));
            }
        } catch (Exception e) {
            log.error("Error deserializing schema registry message.", e);
        }

        return Optional.empty();
    }

    private String toAtlasFormat(SchemaRegistryKey registryKey, SchemaRegistryValue registryVal) {
        switch(registryKey.getKeyType()) {
            case SCHEMA:
                return toAtlasSchema(registryKey, (SchemaValue) registryVal);
            case DELETE_SUBJECT:
                return "";//toAtlasDelete(registryKey, (DeleteSubjectValue) registryVal);
        }

        throw new RuntimeException("No supported ATLAS hook for registry updates of " + registryKey.getKeyType());
    }

    protected String notifyEntities(String user, Collection<Referenceable> entities) {
        JSONArray entitiesArray = new JSONArray();

        for (Referenceable entity : entities) {
            log.info("Adding entity for type: {}", entity.getTypeName());
            final String entityJson = InstanceSerialization.toJson(entity, true);
            entitiesArray.put(entityJson);
        }

        return AbstractNotification.GSON.toJson(new HookNotification.EntityCreateRequest(user, entitiesArray));
    }

    private String toAtlasSchema(SchemaRegistryKey registryKey, SchemaValue registryVal) {
        Schema avroSchema = new Schema.Parser().parse(registryVal.getSchema());
        List<Referenceable> entities = new LinkedList<>();

        Referenceable topicSchema = avroEntityProvider.avroType(avroSchema, registryVal.getSubject());
        Referenceable topicRef = kafkaEntityProvider.topicEntity(registryVal, null, topicSchema);

        if(avroSchema.getProp("connect.name") != null) {
            kafkaEntityProvider.connector(registryVal, ImmutableSet.of(topicRef));
        }

        entities.addAll(avroEntityProvider.allEntities());
        entities.addAll(kafkaEntityProvider.allEntities());

        return notifyEntities("admin", entities);
    }

    private HookNotification.HookNotificationMessage toAtlasDelete(SchemaRegistryKey registryKey, DeleteSubjectValue registryVal) {
        return null;
    }


}
