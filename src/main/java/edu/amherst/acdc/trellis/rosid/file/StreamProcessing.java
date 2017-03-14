/*
 * Copyright Amherst College
 *
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
package edu.amherst.acdc.trellis.rosid.file;

import static edu.amherst.acdc.trellis.rosid.file.FileUtils.partition;
import static java.time.Instant.now;
import static java.util.stream.Stream.empty;
import static java.util.stream.Stream.of;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.KEY_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.VALUE_SERDE_CLASS_CONFIG;
import static org.slf4j.LoggerFactory.getLogger;

import edu.amherst.acdc.trellis.rosid.MessageSerde;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;

/**
 * @author acoburn
 */
final class StreamProcessing {

    private static final Logger LOGGER = getLogger(StreamProcessing.class);

    private static final Long WINDOW_SIZE = Long.parseLong(System.getProperty("kafka.window.delay.ms", "5000"));
    // 2^12-1
    private static final Integer CACHE_SIZE = Integer.parseInt(System.getProperty("kafka.window.cache.size", "4095"));
    private static final String TOPIC_RECACHE = "trellis.cache";
    private static final String TOPIC_UPDATE = "trellis.update";
    private static final String TOPIC_DELETE = "trellis.delete";
    private static final String TOPIC_LDP_CONTAINER_ADD = "trellis.ldpcontainer.add";
    private static final String TOPIC_LDP_CONTAINER_DELETE = "trellis.ldpcontainer.delete";

    /**
     * Configure the KafkaStream processor
     * @param directory the base directory
     * @return the configured kafka stream processor
     */
    public static KafkaStreams configure(final File directory) {
        final String bootstrapServers = System.getProperty("kafka.bootstrap.servers");

        final Map<String, Object> props = new HashMap<>();
        props.put(APPLICATION_ID_CONFIG, "trellis-repository-application");
        props.put(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.put(VALUE_SERDE_CLASS_CONFIG, MessageSerde.class);

        final StateStoreSupplier cachingStore = Stores.create("caching").withStringKeys().withStringValues()
            .inMemory().maxEntries(CACHE_SIZE).build();

        final KStreamBuilder builder = new KStreamBuilder();
        builder.addStateStore(cachingStore);

        builder.stream(TOPIC_UPDATE).foreach((k, v) -> {
            // TODO
            // -- change to transform()
            // -- write dataset to resource
            // -- add prov:endedAtTime
            // -- to("trellis.cache")
        });
        builder.stream(TOPIC_DELETE).foreach((k, v) -> {
            // TODO
            // -- change to transform
            // -- get child resources
            // -- get parent resources
            // -- delete resource
            // -- delete child resources
            // -- re-cache parent, if it exists
        });

        builder.stream(TOPIC_LDP_CONTAINER_ADD)
            .map((k, v) -> {
                final String identifier = (String) k;
                final String quad = (String) v;
                final File dir = new File(directory, partition(identifier));
                try {
                    VersionedResource.write(dir, empty(), of(quad), now());
                } catch (final IOException ex) {
                    LOGGER.error("Error adding LDP container triples to {}: {}", identifier, ex.getMessage());
                }
                return new KeyValue<>(k, null);
            })
            .to(TOPIC_RECACHE);

        builder.stream(TOPIC_LDP_CONTAINER_DELETE)
            .map((k, v) -> {
                final String identifier = (String) k;
                final String quad = (String) v;
                final File dir = new File(directory, partition(identifier));
                try {
                    VersionedResource.write(dir, of(quad), empty(), now());
                } catch (final IOException ex) {
                    LOGGER.error("Error removing LDP container triples from {}: {}", identifier, ex.getMessage());
                }
                return new KeyValue<>(k, null);
            })
            .to(TOPIC_RECACHE);

        // Cache the resource
        builder.stream(TOPIC_RECACHE).groupByKey()
            .reduce((val1, val2) -> val1, TimeWindows.of(WINDOW_SIZE), "caching")
            .foreach((k, v) -> {
                final String identifier = (String) k.key();
                final File dir = new File(directory, partition(identifier));
                try {
                    CachedResource.write(dir, identifier);
                } catch (final IOException ex) {
                    LOGGER.error("Error writing cache for {}: {}", identifier, ex.getMessage());
                }
        });

        return new KafkaStreams(builder, new StreamsConfig(props));
    }

    private StreamProcessing() {
        // prevent instantiation
    }
}
