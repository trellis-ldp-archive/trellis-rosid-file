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

import static edu.amherst.acdc.trellis.rosid.file.Constants.RESOURCE_CACHE;
import static edu.amherst.acdc.trellis.rosid.file.FileUtils.partition;
import static java.time.Instant.now;
import static java.util.Objects.requireNonNull;
import static java.util.Optional.of;
import static java.util.stream.Stream.empty;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.KEY_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.VALUE_SERDE_CLASS_CONFIG;
import static org.slf4j.LoggerFactory.getLogger;

import edu.amherst.acdc.trellis.api.Resource;
import edu.amherst.acdc.trellis.rosid.AbstractResourceService;
import edu.amherst.acdc.trellis.rosid.Message;
import edu.amherst.acdc.trellis.rosid.MessageSerde;
import edu.amherst.acdc.trellis.spi.Session;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import org.apache.commons.rdf.api.IRI;
import org.apache.kafka.clients.producer.Producer;
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
public class FileResourceService extends AbstractResourceService {

    private static final Logger LOGGER = getLogger(FileResourceService.class);

    private final File directory;

    private final KafkaStreams kstreams;

    /**
     * Create a File-based repository service
     * @throws IOException if the directory is not writable
     */
    public FileResourceService() throws IOException {
        super();
        final String path = System.getProperty("trellis.data");
        requireNonNull(path, "trellis.data is unset!");
        this.directory = new File(path);
        initFiles();

        this.kstreams = configureStreams();
        this.kstreams.start();
    }

    /**
     * Create a File-based repository service
     * @param directory the data directory
     * @param producer the kafka producer
     * @throws IOException if the directory is not writable
     */
    protected FileResourceService(final File directory, final Producer<String, Message> producer,
            final KafkaStreams streams) throws IOException {
        super(producer);
        requireNonNull(directory, "directory may not be null!");
        requireNonNull(streams, "streams may not be null!");
        this.directory = directory;
        initFiles();

        this.kstreams = streams;
        this.kstreams.start();
    }

    @Override
    public Optional<Resource> get(final Session session, final IRI identifier) {
        // this ignores the session (e.g. batch ops)
        return of(new File(directory, partition(identifier))).filter(File::exists)
            .flatMap(dir -> new File(dir, RESOURCE_CACHE).exists() ?
                    CachedResource.find(dir, identifier) : VersionedResource.find(dir, identifier, now()));
    }

    @Override
    public Optional<Resource> get(final Session session, final IRI identifier, final Instant time) {
        // this ignores the session (e.g. batch ops)
        return of(new File(directory, partition(identifier))).filter(File::exists)
            .flatMap(dir -> VersionedResource.find(dir, identifier, time));
    }

    private void initFiles() throws IOException {
        if (!directory.exists()) {
            directory.mkdirs();
        }
        if (!directory.canWrite()) {
            throw new IOException("Cannot write to " + directory.getAbsolutePath());
        }

        LOGGER.info("Using base data directory: {}", directory.getAbsolutePath());
    }

    private KafkaStreams configureStreams() {
        final String bootstrapServers = System.getProperty("kafka.bootstrap.servers");
        final Map<String, Object> props = new HashMap<>();
        props.put(APPLICATION_ID_CONFIG, "trellis-repository-application");
        props.put(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.put(VALUE_SERDE_CLASS_CONFIG, MessageSerde.class);

        final StateStoreSupplier cachingStore = Stores.create("caching").withStringKeys().withStringValues()
            .inMemory().maxEntries((int)Math.pow(2, 8) - 1).build();

        final KStreamBuilder builder = new KStreamBuilder();
        builder.addStateStore(cachingStore);

        builder.stream("trellis.update").foreach((k, v) -> {
            // TODO
            // -- change to transform()
            // -- write dataset to resource
            // -- add prov:endedAtTime
            // -- to("trellis.cache")
        });
        builder.stream("trellis.delete").foreach((k, v) -> {
            // TODO
            // -- change to transform
            // -- get child resources
            // -- get parent resources
            // -- delete resource
            // -- delete child resources
            // -- re-cache parent, if it exists
        });

        builder.stream("trellis.ldpcontainer.add").map((k, v) -> {
            final String identifier = (String) k;
            final String quad = (String) v;
            final File dir = new File(directory, partition(identifier));
            try {
                VersionedResource.write(dir, empty(), Stream.of(quad), now());
            } catch (final IOException ex) {
                LOGGER.error("Error writing to {}: {}", identifier, ex.getMessage());
            }
            return new KeyValue<>(k, null);
        }).to("trellis.cache");

        builder.stream("trellis.ldpcontainer.delete").map((k, v) -> {
            final String identifier = (String) k;
            final String quad = (String) v;
            final File dir = new File(directory, partition(identifier));
            try {
                VersionedResource.write(dir, Stream.of(quad), empty(), now());
            } catch (final IOException ex) {
                LOGGER.error("Error writing to {}: {}", identifier, ex.getMessage());
            }
            return new KeyValue<>(k, null);
        }).to("trellis.cache");

        // Cache the resource
        builder.stream("trellis.cache").groupByKey().reduce((val1, val2) -> val1, TimeWindows.of(5000L), "caching")
            .foreach((k, v) -> {
                final String identifier = (String) k.key();
                final File dir = new File(directory, partition(identifier));
                try {
                    CachedResource.write(dir, identifier);
                } catch (final IOException ex) {
                    LOGGER.error("Error writing to {}: {}", identifier, ex.getMessage());
                }
        });

        return new KafkaStreams(builder, new StreamsConfig(props));
    }

    @Override
    public void close() {
        super.close();
        kstreams.close();
    }
}
