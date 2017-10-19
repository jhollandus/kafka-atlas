package com.github.jhollandus.kafka.atlas.stream;

import java.util.List;

public class DemoStreamConfig {
    private String atlasUrl = "http://localhost:21000";
    private String kafkaCluster;
    private String kafkaClusterLocation;
    private String appId;
    private List<String> inputTopics;
    private List<String> outputTopics;

    public String getAppId() {
        return appId;
    }

    public DemoStreamConfig withAppId(String appId) {
        this.appId = appId;
        return this;
    }

    public String getKafkaClusterLocation() {
        return kafkaClusterLocation;
    }

    public DemoStreamConfig withKafkaClusterLocation(String kafkaClusterLocation) {
        this.kafkaClusterLocation = kafkaClusterLocation;
        return this;
    }

    public String getAtlasUrl() {
        return atlasUrl;
    }

    public DemoStreamConfig withAtlasUrl(String atlasUrl) {
        this.atlasUrl = atlasUrl;
        return this;
    }

    public String getKafkaCluster() {
        return kafkaCluster;
    }

    public DemoStreamConfig withKafkaCluster(String kafkaCluster) {
        this.kafkaCluster = kafkaCluster;
        return this;
    }

    public List<String> getInputTopics() {
        return inputTopics;
    }

    public DemoStreamConfig withInputTopics(List<String> inputTopics) {
        this.inputTopics = inputTopics;
        return this;
    }

    public List<String> getOutputTopics() {
        return outputTopics;
    }

    public DemoStreamConfig withOutputTopics(List<String> outputTopics) {
        this.outputTopics = outputTopics;
        return this;
    }
}
