package com.github.jhollandus.kafka.atlas;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.confluent.kafka.schemaregistry.storage.SchemaValue;
import org.apache.atlas.AtlasClient;
import org.apache.atlas.typesystem.Referenceable;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;


public class KafkaEntityProvider {
    public static final String ATLAS_TYPE_KAFKA_CLUSTER = "kafka_cluster";
    public static final String ATLAS_TYPE_KAFKA_TOPIC = "kafka_topic";
    public static final String ATLAS_TYPE_KAFKA_STREAMS = "kafka_streams_process";
    public static final String ATLAS_TYPE_KAFKA_CONNECT = "kafka_connect_process";

    private static final Joiner NAME_JOINER = Joiner.on('.');
    private static final Joiner SUBJECT_JOINER = Joiner.on('-');
    private static final Splitter SUBJECT_SPLITTER = Splitter.on('-');
    //naming convention: cdc-db-<databasename>-<table>-<value/key> for connectors

    private final Referenceable cluster;
    private final String clusterName;
    private final Map<String, Referenceable> topics = new HashMap<>();
    private final Map<String, Referenceable> connectors = new HashMap<>();
    private final Map<String, Referenceable> streams = new HashMap<>();
    private final Map<String, Referenceable> databases = new HashMap<>();
    private final Map<String, Referenceable> tables = new HashMap<>();

    public KafkaEntityProvider(String clusterName, String clusterLocation) {
        this.clusterName = clusterName;

        cluster = new Referenceable(ATLAS_TYPE_KAFKA_CLUSTER);
        cluster.set(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, clusterName);
        cluster.set("clusterName", clusterName);
        cluster.set("name", clusterName);
        cluster.set("location", clusterLocation);
    }

    public List<Referenceable> allEntities() {
        List<Referenceable> all = new LinkedList<>();
        all.addAll(topics.values());
        all.addAll(connectors.values());
        all.addAll(streams.values());
        all.addAll(databases.values());
        all.addAll(tables.values());
        all.add(cluster);

        return all;
    }

    public Referenceable connector(SchemaValue schemaVal, Set<Referenceable> outputs) {
        String topicName = extractTopicName(schemaVal);
        List<String> parts = SUBJECT_SPLITTER.splitToList(topicName);

        //TODO: determine if this is a database connector or not
        String category = parts.get(0);
        String source = parts.get(1);
        String database  = parts.get(2);
        String table = parts.get(3);
        String name = SUBJECT_JOINER.join(category, source, database);
        String id = NAME_JOINER.join(clusterName, name);

        Referenceable db = database(database);
        Referenceable tableRef = table(db, table);

        Referenceable connectRef = connectors.computeIfAbsent(id, k -> {
            Referenceable connector = new Referenceable(ATLAS_TYPE_KAFKA_CONNECT);
            connector.set(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, id);
            connector.set("cluster", cluster);
            connector.set("name", name);
            connector.set("inputs", ImmutableList.of(tableRef));
            connector.set("outputs", ImmutableList.copyOf(outputs));

            return connector;
        });

        Set<Referenceable> inputRefs = new HashSet<>();
        inputRefs.addAll((List) connectRef.get("inputs"));
        inputRefs.add(tableRef);
        connectRef.set("inputs", ImmutableList.copyOf(inputRefs));

        Set<Referenceable> outputRefs = new HashSet<>();
        outputRefs.addAll((List) connectRef.get("outputs"));
        outputRefs.addAll(outputs);
        connectRef.set("outputs", ImmutableList.copyOf(outputRefs));

        return connectors.put(id, connectRef);
    }

    private Referenceable database(String name) {
        return databases.computeIfAbsent(name, k -> {
            Referenceable database = new Referenceable("database");
            database.set(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, name);
            database.set("name", name);

            return database;
        });
    }

    private Referenceable table(Referenceable db, String name) {
        String id = NAME_JOINER.join(db.get("name").toString(), name);
        return tables.computeIfAbsent(id, k -> {
            Referenceable table = new Referenceable("table");
            table.set(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, id);
            table.set("name", name);
            table.set("database", db);

            return table;
        });
    }

    public Referenceable topicEntity(SchemaValue schemaVal, Referenceable topicKeySchema, Referenceable topicValueSchema) {
        String topicName = extractTopicName(schemaVal);
        String id = NAME_JOINER.join(clusterName, topicName);

        return topics.computeIfAbsent(id, k -> {
            Referenceable topic = new Referenceable(ATLAS_TYPE_KAFKA_TOPIC);
            topic.set(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, id);
            topic.set("cluster", cluster);
            topic.set("topicName", topicName);
            topic.set("name", topicName);

            if (topicKeySchema != null) {
                topic.set("keySchema", topicKeySchema);
            }

            topic.set("valueSchema", topicValueSchema);
            return topic;
        });
    }

    private String extractTopicName(SchemaValue schemaVal) {
        return schemaVal.getSubject().substring(0, schemaVal.getSubject().lastIndexOf('-'));
    }
}
