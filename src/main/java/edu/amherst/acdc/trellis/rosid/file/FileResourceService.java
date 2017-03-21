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
import static edu.amherst.acdc.trellis.rosid.file.Constants.RESOURCE_JOURNAL;
import static edu.amherst.acdc.trellis.rosid.file.FileUtils.resourceDirectory;
import static java.time.Instant.now;
import static java.util.Objects.requireNonNull;
import static java.util.Optional.of;
import static java.util.stream.Collectors.toMap;
import static org.slf4j.LoggerFactory.getLogger;

import com.fasterxml.jackson.databind.ObjectMapper;

import edu.amherst.acdc.trellis.api.Resource;
import edu.amherst.acdc.trellis.rosid.common.AbstractResourceService;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import org.apache.commons.rdf.api.Dataset;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.streams.KafkaStreams;
import org.slf4j.Logger;

/**
 * @author acoburn
 */
public class FileResourceService extends AbstractResourceService {

    private static final Logger LOGGER = getLogger(FileResourceService.class);

    private final Configuration configuration;

    private final Map<String, String> storageConfig;

    private final KafkaStreams kstreams;

    /**
     * Create a File-based repository service
     * @throws IOException if the directory is not writable
     */
    public FileResourceService() throws IOException {
        super();
        final String config = System.getProperty("trellis.configuration");
        requireNonNull(config, "trellis.configuration is unset!");
        final File configFile = new File(config);
        if (!configFile.exists()) {
            throw new IOException("Configuration file (" + config + ") does not exist");
        }
        final ObjectMapper mapper = new ObjectMapper();
        this.configuration = mapper.readValue(new File(config), Configuration.class);
        this.storageConfig = this.configuration.storage.entrySet().stream()
                .collect(toMap(e -> e.getKey(), e -> e.getValue().get("resources")));


        init();

        this.kstreams = StreamConfiguration.configure(storageConfig);
        this.kstreams.start();
    }

    /**
     * Create a File-based repository service
     * @param configuration the configuration
     * @param producer the kafka producer
     * @param streams the kafka streams
     * @throws IOException if the directory is not writable
     */
    protected FileResourceService(final Configuration configuration, final Producer<String, Dataset> producer,
            final KafkaStreams streams) throws IOException {
        super(producer);
        requireNonNull(configuration, "configuration may not be null!");
        requireNonNull(streams, "streams may not be null!");
        this.configuration = configuration;
        this.storageConfig = this.configuration.storage.entrySet().stream()
                .collect(toMap(e -> e.getKey(), e -> e.getValue().get("resources")));

        init();

        this.kstreams = streams;
        this.kstreams.start();
    }

    @Override
    public Optional<Resource> get(final IRI identifier) {
        return of(resourceDirectory(storageConfig, identifier)).filter(File::exists)
            .flatMap(dir -> new File(dir, RESOURCE_CACHE).exists() ?
                    CachedResource.find(dir, identifier) : VersionedResource.find(dir, identifier, now()));
    }

    @Override
    public Optional<Resource> get(final IRI identifier, final Instant time) {
        return of(resourceDirectory(storageConfig, identifier)).filter(File::exists)
            .flatMap(dir -> VersionedResource.find(dir, identifier, time));
    }

    @Override
    protected Boolean write(final IRI identifier, final Stream<? extends Quad> remove,
            final Stream<? extends Quad> add, final Instant time) {
        final File dir = resourceDirectory(storageConfig, identifier);
        try {
            RDFPatch.write(new File(dir, RESOURCE_JOURNAL), remove, add, time);
            return true;
        } catch (final IOException ex) {
            LOGGER.error("Error writing to resource '{}': {}", identifier.getIRIString(), ex.getMessage());
            return false;
        }
    }

    @Override
    public void close() {
        super.close();
        kstreams.close();
    }

    private void init() throws IOException {
        for (final Map.Entry<String, String> storage : storageConfig.entrySet()) {
            final File res = new File(URI.create(storage.getValue()));
            if (!res.exists()) {
                res.mkdirs();
            }
            if (!res.canWrite()) {
                throw new IOException("Cannot write to " + res.getAbsolutePath());
            }

            LOGGER.info("Using resource data directory for '{}': {}", storage.getKey(), res.getAbsolutePath());
        }
    }
}
