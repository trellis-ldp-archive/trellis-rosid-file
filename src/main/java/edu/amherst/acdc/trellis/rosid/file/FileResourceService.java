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
import static org.slf4j.LoggerFactory.getLogger;

import edu.amherst.acdc.trellis.api.Resource;
import edu.amherst.acdc.trellis.rosid.AbstractResourceService;
import edu.amherst.acdc.trellis.rosid.Message;
import edu.amherst.acdc.trellis.spi.Session;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.Optional;

import org.apache.commons.rdf.api.IRI;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.streams.KafkaStreams;
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
        directory = new File(path);
        initFiles();

        kstreams = StreamProcessing.configure(directory);
        kstreams.start();
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

    @Override
    public void close() {
        super.close();
        kstreams.close();
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

}
