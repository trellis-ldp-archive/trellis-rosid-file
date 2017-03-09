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
package edu.amherst.acdc.trellis.rosid;

import static edu.amherst.acdc.trellis.rosid.Constants.RESOURCE_CACHE;
import static edu.amherst.acdc.trellis.rosid.FileUtils.partition;
import static java.time.Instant.now;
import static java.util.Objects.requireNonNull;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static org.slf4j.LoggerFactory.getLogger;

import edu.amherst.acdc.trellis.api.Resource;
import edu.amherst.acdc.trellis.spi.EventService;
import edu.amherst.acdc.trellis.spi.ResourceService;
import edu.amherst.acdc.trellis.spi.Session;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;

import org.apache.commons.rdf.api.Graph;
import org.apache.commons.rdf.api.IRI;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;

/**
 * @author acoburn
 */
public class FileResourceService implements ResourceService, AutoCloseable {

    private static final Logger LOGGER = getLogger(FileResourceService.class);

    private EventService evtSvc;
    private File directory;
    private Producer<String, Message> producer;
    private Consumer<String, Message> consumer;

    /**
     * Create a File-based repository service, using system properties
     * @throws IOException if the directory is not writable
     */
    public FileResourceService() throws IOException {
        this(new File(System.getProperty("trellis.data")),
            new KafkaProducer<>(kafkaProducerProps(), new StringSerializer(), new MessageSerializer()),
            new KafkaConsumer<>(kafkaConsumerProps(), new StringDeserializer(), new MessageSerializer()));
    }

    /**
     * Create a File-based repository service
     * @param directory the data directory
     * @param producer the kafka producer
     * @param consumer the kafka consumer
     * @throws IOException if the directory is not writable
     */
    public FileResourceService(final File directory, final Producer<String, Message> producer,
            final Consumer<String, Message> consumer) throws IOException {
        requireNonNull(directory, "directory may not be null!");

        if (!directory.exists()) {
            directory.mkdirs();
        }
        if (!directory.canWrite()) {
            throw new IOException("Cannot write to " + directory.getAbsolutePath());
        }
        this.directory = directory;
        this.consumer = consumer;
        this.producer = producer;
    }

    @Override
    public void bind(final EventService svc) {
        LOGGER.info("Binding EventService to RepositoryService");
        evtSvc = svc;
    }

    @Override
    public void unbind(final EventService svc) {
        if (Objects.equals(evtSvc, svc)) {
            LOGGER.info("Unbinding EventService from RepositoryService");
            evtSvc = null;
        }
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
    public Boolean put(final Session session, final IRI identifier, final IRI type, final Graph graph) {
        final File dir = new File(directory, partition(identifier));
        if (!dir.exists()) {
            dir.mkdirs();
        }

        final Message msg = new Message(identifier, type, graph);
        producer.send(new ProducerRecord<>("create", identifier.getIRIString(), msg));

        // TODO -- add zk lock
        return true;
    }

    @Override
    public void delete(final Session session, final IRI identifier) {
        // TODO
    }

    @Override
    public void commit(final Session session) {
        // TODO
    }

    @Override
    public void expire(final Session session) {
        // TODO
    }

    @Override
    public Session begin() {
        // TODO
        return null;
    }

    @Override
    public Optional<Session> resume(final IRI identifier) {
        // TODO
        return empty();
    }

    @Override
    public Optional<Session> extend(final Session session, final Duration duration) {
        // TODO
        return empty();
    }

    @Override
    public void close() {
        // TODO -- close any ZK connections
        consumer.close();
        producer.close();
    }

    private static Properties kafkaConsumerProps() {
        final Properties props = new Properties();
        props.put("bootstrap.servers", System.getProperty("kafka.bootstrap.servers"));
        props.put("group.id", System.getProperty("kafka.group.id"));
        props.put("enable.auto.commit", System.getProperty("kafka.enable.auto.commit", "false"));
        props.put("auto.commit.interval.ms", System.getProperty("kafka.auto.commit.interval.ms", "1000"));
        props.put("session.timeout.ms", System.getProperty("kafka.session.timeout.ms", "30000"));
        return props;
    }

    private static Properties kafkaProducerProps() {
        final Properties props = new Properties();
        props.put("bootstrap.servers", System.getProperty("kafka.bootstrap.servers"));
        props.put("acks", System.getProperty("kafka.acks", "all"));
        props.put("retries", System.getProperty("kafka.retries", "0"));
        props.put("batch.size", System.getProperty("kafka.batch.size", "16384"));
        props.put("linger.ms", System.getProperty("kafka.linger.ms", "1"));
        props.put("buffer.memory", System.getProperty("kafka.buffer.memory", "33554432"));
        return props;
    }
}
