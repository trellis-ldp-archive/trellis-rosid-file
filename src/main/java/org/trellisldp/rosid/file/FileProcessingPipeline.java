/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.trellisldp.rosid.file;

import static java.lang.Long.parseLong;
import static org.trellisldp.rosid.common.RosidConstants.TOPIC_CACHE;
import static org.trellisldp.rosid.common.RosidConstants.TOPIC_CACHE_AGGREGATE;
import static org.trellisldp.rosid.common.RosidConstants.TOPIC_INBOUND_ADD;
import static org.trellisldp.rosid.common.RosidConstants.TOPIC_INBOUND_DELETE;
import static org.trellisldp.rosid.common.RosidConstants.TOPIC_LDP_CONTAINMENT_ADD;
import static org.trellisldp.rosid.common.RosidConstants.TOPIC_LDP_CONTAINMENT_DELETE;
import static org.trellisldp.rosid.common.RosidConstants.TOPIC_LDP_MEMBERSHIP_ADD;
import static org.trellisldp.rosid.common.RosidConstants.TOPIC_LDP_MEMBERSHIP_DELETE;
import static org.trellisldp.rosid.file.FileUtils.getPropertySection;
import static org.trellisldp.rosid.file.FileUtils.getStorageConfig;

import java.util.Map;
import java.util.Properties;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import org.joda.time.Duration;

import org.trellisldp.vocabulary.Fedora;
import org.trellisldp.vocabulary.LDP;

/**
 * @author acoburn
 */
public class FileProcessingPipeline {

    private static final String STORAGE_PREFIX = "trellis.storage.";
    private static final String KAFKA_PREFIX = "kafka.";

    private final Map<String, String> config;
    private final String bootstrapServers;
    private final long cacheSeconds;

    /**
     * Build a file processing pipeline
     * @param configuration the configuration
     */
    public FileProcessingPipeline(final Properties configuration) {
        this.config = getStorageConfig(getPropertySection(configuration, STORAGE_PREFIX), ".resources");
        this.bootstrapServers = configuration.getProperty(KAFKA_PREFIX + "bootstrapServers");
        this.cacheSeconds = parseLong(configuration.getProperty("trellis.cache.seconds", "5"));
    }

    /**
     * Get the beam pipeline
     * @return the pipeline
     */
    public Pipeline getPipeline() {

        final PipelineOptions options = PipelineOptionsFactory.create();
        final Pipeline p = Pipeline.create(options);

        p.apply(KafkaIO.<String, String>read().withBootstrapServers(bootstrapServers)
                    .withKeyDeserializer(StringDeserializer.class)
                    .withValueDeserializer(StringDeserializer.class)
                    .withTopic(TOPIC_INBOUND_ADD).withoutMetadata())
            .apply(ParDo.of(new BeamProcessor(config, Fedora.PreferInboundReferences.getIRIString(), true)));

        p.apply(KafkaIO.<String, String>read().withBootstrapServers(bootstrapServers)
                    .withKeyDeserializer(StringDeserializer.class)
                    .withValueDeserializer(StringDeserializer.class)
                    .withTopic(TOPIC_INBOUND_DELETE).withoutMetadata())
            .apply(ParDo.of(new BeamProcessor(config, Fedora.PreferInboundReferences.getIRIString(), false)));

        p.apply(KafkaIO.<String, String>read().withBootstrapServers(bootstrapServers)
                    .withKeyDeserializer(StringDeserializer.class)
                    .withValueDeserializer(StringDeserializer.class)
                    .withTopic(TOPIC_LDP_MEMBERSHIP_ADD).withoutMetadata())
            .apply(ParDo.of(new BeamProcessor(config, LDP.PreferMembership.getIRIString(), true)))
            .apply(KafkaIO.<String, String>write().withBootstrapServers(bootstrapServers)
                    .withKeySerializer(StringSerializer.class)
                    .withValueSerializer(StringSerializer.class)
                    .withTopic(TOPIC_CACHE_AGGREGATE));

        p.apply(KafkaIO.<String, String>read().withBootstrapServers(bootstrapServers)
                    .withKeyDeserializer(StringDeserializer.class)
                    .withValueDeserializer(StringDeserializer.class)
                    .withTopic(TOPIC_LDP_MEMBERSHIP_DELETE).withoutMetadata())
            .apply(ParDo.of(new BeamProcessor(config, LDP.PreferMembership.getIRIString(), false)))
            .apply(KafkaIO.<String, String>write().withBootstrapServers(bootstrapServers)
                    .withKeySerializer(StringSerializer.class)
                    .withValueSerializer(StringSerializer.class)
                    .withTopic(TOPIC_CACHE_AGGREGATE));

        p.apply(KafkaIO.<String, String>read().withBootstrapServers(bootstrapServers)
                    .withKeyDeserializer(StringDeserializer.class)
                    .withValueDeserializer(StringDeserializer.class)
                    .withTopic(TOPIC_LDP_CONTAINMENT_ADD).withoutMetadata())
            .apply(ParDo.of(new BeamProcessor(config, LDP.PreferContainment.getIRIString(), true)))
            .apply(KafkaIO.<String, String>write().withBootstrapServers(bootstrapServers)
                    .withKeySerializer(StringSerializer.class)
                    .withValueSerializer(StringSerializer.class)
                    .withTopic(TOPIC_CACHE_AGGREGATE));

        p.apply(KafkaIO.<String, String>read().withBootstrapServers(bootstrapServers)
                    .withKeyDeserializer(StringDeserializer.class)
                    .withValueDeserializer(StringDeserializer.class)
                    .withTopic(TOPIC_LDP_CONTAINMENT_DELETE).withoutMetadata())
            .apply(ParDo.of(new BeamProcessor(config, LDP.PreferContainment.getIRIString(), false)))
            .apply(KafkaIO.<String, String>write().withBootstrapServers(bootstrapServers)
                    .withKeySerializer(StringSerializer.class)
                    .withValueSerializer(StringSerializer.class)
                    .withTopic(TOPIC_CACHE_AGGREGATE));

        p.apply(KafkaIO.<String, String>read().withBootstrapServers(bootstrapServers)
                    .withKeyDeserializer(StringDeserializer.class)
                    .withValueDeserializer(StringDeserializer.class)
                    .withTopic(TOPIC_CACHE_AGGREGATE).withoutMetadata())
            .apply(Window.<KV<String, String>>into(FixedWindows.of(Duration.standardSeconds(cacheSeconds))))
            .apply(Combine.perKey(x -> x.iterator().next()))
            .apply(KafkaIO.<String, String>write().withBootstrapServers(bootstrapServers)
                    .withKeySerializer(StringSerializer.class)
                    .withValueSerializer(StringSerializer.class)
                    .withTopic(TOPIC_CACHE));

        p.apply(KafkaIO.<String, String>read().withBootstrapServers(bootstrapServers)
                    .withKeyDeserializer(StringDeserializer.class)
                    .withValueDeserializer(StringDeserializer.class)
                    .withTopic(TOPIC_CACHE))
            .apply(ParDo.of(new CacheWriter(config)));

        return p;
    }

}
