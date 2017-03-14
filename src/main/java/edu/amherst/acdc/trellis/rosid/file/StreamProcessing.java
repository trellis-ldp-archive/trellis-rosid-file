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

import static edu.amherst.acdc.trellis.rosid.common.Constants.TOPIC_DELETE;
import static edu.amherst.acdc.trellis.rosid.common.Constants.TOPIC_LDP_CONTAINER_ADD;
import static edu.amherst.acdc.trellis.rosid.common.Constants.TOPIC_LDP_CONTAINER_DELETE;
import static edu.amherst.acdc.trellis.rosid.common.Constants.TOPIC_RECACHE;
import static edu.amherst.acdc.trellis.rosid.common.Constants.TOPIC_UPDATE;
import static edu.amherst.acdc.trellis.rosid.file.FileUtils.partition;
import static java.time.Instant.now;
import static java.util.stream.Stream.empty;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.streams.kstream.TimeWindows.of;
import static org.slf4j.LoggerFactory.getLogger;

import edu.amherst.acdc.trellis.rosid.common.DatasetSerde;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.commons.rdf.api.Dataset;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Windowed;
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
    private static final String CACHE_NAME = "trellis.caching";

    /**
     * A mapping function for updating LDP Container properties
     * @param directory the base resource directory
     * @return a mapping function that generates an additional message for further processing
     */
    public static KeyValueMapper<String, Dataset, KeyValue<String, Dataset>> ldpAdder(final File directory) {
        return (identifier, dataset) -> {
            final File dir = new File(directory, partition(identifier));
            try {
                RDFPatch.write(dir, empty(), dataset.stream(), now());
            } catch (final IOException ex) {
                LOGGER.error("Error adding LDP container triples to {}: {}", identifier, ex.getMessage());
            }
            return new KeyValue<>(identifier, null);
        };
    }

    /**
     * A mapping function for deleting LDP Container properties
     * @param directory the base resource directory
     * @return a mapping function that generates an additional message for further processing
     */
    public static KeyValueMapper<String, Dataset, KeyValue<String, Dataset>> ldpDeleter(final File directory) {
        return (identifier, dataset) -> {
            final File dir = new File(directory, partition(identifier));
            try {
                RDFPatch.write(dir, dataset.stream(), empty(), now());
            } catch (final IOException ex) {
                LOGGER.error("Error removing LDP container triples from {}: {}", identifier, ex.getMessage());
            }
            return new KeyValue<>(identifier, null);
        };
    }

    /**
     * A mapping function for updating the resource cache
     * @param directory the base resource directory
     * @return a terminating foreach action
     */
    public static ForeachAction<Windowed<String>, Dataset> cacheWriter(final File directory) {
        return (window, data) -> {
            final String identifier = window.key();
            final File dir = new File(directory, partition(identifier));
            try {
                CachedResource.write(dir, identifier);
            } catch (final IOException ex) {
                LOGGER.error("Error writing cache for {}: {}", identifier, ex.getMessage());
            }
        };
    }

    /**
     * A mapping function for updating resources
     * @param directory the base resource directory
     * @return a mapping function that generates 0 or more messages for further processing
     */
    public static KeyValueMapper<String, Dataset, Iterable<KeyValue<String, Dataset>>> updater(final File directory) {
        // TODO
        // -- write dataset to resource
        // -- add prov:endedAtTime
        // -- re-cache parent, if this is new
        return (identifier, dataset) -> {
            final Stream<KeyValue<String, Dataset>> stream = empty();
            return new Iterable<KeyValue<String, Dataset>>() {
                @Override
                public Iterator<KeyValue<String, Dataset>> iterator() {
                    return stream.iterator();
                }
            };
        };
    }

    /**
     * A mapping function for deleting resources
     * @param directory the base resource directory
     * @return a mapping function that generates 0 or more messages for further processing
     */
    public static KeyValueMapper<String, Dataset, Iterable<KeyValue<String, Dataset>>> deleter(final File directory) {
        // TODO
        // -- get child resources
        // -- get parent resources
        // -- delete resource
        // -- delete child resources
        // -- re-cache parent, if it exists
        return (identifier, dataset) -> {
            final Stream<KeyValue<String, Dataset>> stream = empty();
            return new Iterable<KeyValue<String, Dataset>>() {
                @Override
                public Iterator<KeyValue<String, Dataset>> iterator() {
                    return stream.iterator();
                }
            };
        };
    }

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

        final StateStoreSupplier cachingStore = Stores.create(CACHE_NAME).withStringKeys().withStringValues()
            .inMemory().maxEntries(CACHE_SIZE).build();

        final Serde<String> kserde = new Serdes.StringSerde();
        final Serde<Dataset> vserde = new DatasetSerde();

        final KStreamBuilder builder = new KStreamBuilder();
        builder.addStateStore(cachingStore);
        builder.stream(kserde, vserde, TOPIC_LDP_CONTAINER_ADD).map(ldpAdder(directory)).to(TOPIC_RECACHE);
        builder.stream(kserde, vserde, TOPIC_LDP_CONTAINER_DELETE).map(ldpDeleter(directory)).to(TOPIC_RECACHE);
        builder.stream(kserde, vserde, TOPIC_RECACHE).groupByKey()
            .reduce((val1, val2) -> val1, of(WINDOW_SIZE), CACHE_NAME).foreach(cacheWriter(directory));


        builder.stream(kserde, vserde, TOPIC_UPDATE).flatMap(updater(directory)).to(TOPIC_RECACHE);
        builder.stream(kserde, vserde, TOPIC_DELETE).flatMap(deleter(directory)).to(TOPIC_RECACHE);
        return new KafkaStreams(builder, new StreamsConfig(props));
    }

    private StreamProcessing() {
        // prevent instantiation
    }
}
