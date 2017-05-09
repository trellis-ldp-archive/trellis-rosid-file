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

import static java.time.Instant.now;
import static java.util.Objects.isNull;
import static java.util.Objects.requireNonNull;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Stream.empty;
import static java.util.stream.Stream.of;
import static org.slf4j.LoggerFactory.getLogger;
import static org.trellisldp.rosid.file.Constants.RESOURCE_CACHE;
import static org.trellisldp.rosid.file.Constants.RESOURCE_JOURNAL;
import static org.trellisldp.rosid.file.FileUtils.resourceDirectory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Stream;

import org.apache.commons.rdf.api.Dataset;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.apache.curator.framework.CuratorFramework;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.streams.KafkaStreams;
import org.slf4j.Logger;
import org.trellisldp.api.Resource;
import org.trellisldp.rosid.common.AbstractResourceService;
import org.trellisldp.spi.EventService;
import org.trellisldp.vocabulary.AS;
import org.trellisldp.vocabulary.LDP;
import org.trellisldp.vocabulary.PROV;
import org.trellisldp.vocabulary.RDF;
import org.trellisldp.vocabulary.Trellis;
import org.trellisldp.vocabulary.XSD;

/**
 * @author acoburn
 */
public class FileResourceService extends AbstractResourceService {

    private static final Logger LOGGER = getLogger(FileResourceService.class);

    private final Map<String, String> resourceConfig;
    private final Map<String, String> blobConfig;

    private final KafkaStreams kstreams;

    /**
     * Create a File-based repository service
     * @param service the event service
     * @param configuration the configuration
     * @throws IOException if the directory is not writable
     */
    public FileResourceService(final EventService service, final Properties configuration) throws IOException {
        super(service, getPropertySection(configuration, "kafka."), getPropertySection(configuration, "zk."));
        requireNonNull(configuration, "configuration may not be null!");
        this.resourceConfig = getStorageConfig(getPropertySection(configuration, "trellis.storage."), ".resources");
        this.blobConfig = getStorageConfig(getPropertySection(configuration, "trellis.storage."), ".blobs");

        init();

        this.kstreams = StreamConfiguration.configure(configuration.getProperty("kafka.bootstrap.servers"),
                this.resourceConfig);
        this.kstreams.start();
    }

    /**
     * Create a File-based repository service
     * @param service the event service
     * @param configuration the configuration
     * @param curator the curator framework
     * @param producer the kafka producer
     * @param streams the kafka streams
     * @throws IOException if the directory is not writable
     */
    protected FileResourceService(final EventService service, final Properties configuration,
            final CuratorFramework curator, final Producer<String, Dataset> producer, final KafkaStreams streams)
            throws IOException {
        super(service, producer, curator);
        requireNonNull(configuration, "configuration may not be null!");
        this.resourceConfig = getStorageConfig(getPropertySection(configuration, "trellis.storage."), ".resources");
        this.blobConfig = getStorageConfig(getPropertySection(configuration, "trellis.storage."), ".blobs");

        init();

        this.kstreams = streams;
        this.kstreams.start();
    }

    @Override
    public Optional<Resource> get(final IRI identifier) {
        return ofNullable(resourceDirectory(resourceConfig, identifier)).filter(File::exists)
            .flatMap(dir -> new File(dir, RESOURCE_CACHE).exists() ?
                    CachedResource.find(dir, identifier) : VersionedResource.find(dir, identifier, now()));
    }

    @Override
    public Optional<Resource> get(final IRI identifier, final Instant time) {
        return ofNullable(resourceDirectory(resourceConfig, identifier)).filter(File::exists)
            .flatMap(dir -> VersionedResource.find(dir, identifier, time));
    }

    @Override
    protected Boolean write(final IRI identifier, final Stream<? extends Quad> remove,
            final Stream<? extends Quad> add, final Instant time) {
        final File dir = resourceDirectory(resourceConfig, identifier);
        if (isNull(dir)) {
            return false;
        }
        dir.mkdirs();
        return RDFPatch.write(new File(dir, RESOURCE_JOURNAL), remove, add, time);
    }

    @Override
    public void close() {
        super.close();
        kstreams.close();
    }

    private void init() throws IOException {
        for (final Map.Entry<String, String> storage : resourceConfig.entrySet()) {
            final File data = storage.getValue().startsWith("file:") ?
                 new File(URI.create(storage.getValue())) : new File(storage.getValue());
            LOGGER.info("Using resource data directory for '{}': {}", storage.getKey(), data.getAbsolutePath());
            if (!data.exists()) {
                data.mkdirs();
            }
            if (!data.canWrite()) {
                throw new IOException("Cannot write to " + data.getAbsolutePath());
            }
            final IRI identifier = rdf.createIRI("trellis:" + storage.getKey());
            final File root = resourceDirectory(resourceConfig, identifier);
            final File rootData = new File(root, RESOURCE_JOURNAL);

            if (!root.exists() || !rootData.exists()) {
                LOGGER.info("Initializing root container for '{}'", identifier.getIRIString());
                root.mkdirs();
                final Instant time = now();
                final IRI skolem = (IRI) skolemize(rdf.createBlankNode());
                final Stream<Quad> quads = of(
                        rdf.createQuad(Trellis.PreferServerManaged, identifier, RDF.type, LDP.Container),
                        rdf.createQuad(Trellis.PreferAudit, identifier, PROV.wasGeneratedBy, skolem),
                        rdf.createQuad(Trellis.PreferAudit, skolem, RDF.type, PROV.Activity),
                        rdf.createQuad(Trellis.PreferAudit, skolem, RDF.type, AS.Create),
                        rdf.createQuad(Trellis.PreferAudit, skolem, PROV.generatedAtTime,
                            rdf.createLiteral(time.toString(), XSD.dateTime)));
                RDFPatch.write(rootData, empty(), quads, now());
                CachedResource.write(root, identifier);
            }
        }
    }

    private static Map<String, String> getStorageConfig(final Properties props, final String suffix) {
        // trellis.storage.repo1.resources
        // trellis.storage.repo1.blobs
        return props.stringPropertyNames().stream().filter(key -> key.endsWith(suffix))
            .collect(toMap(key -> key.split("\\.")[0], props::getProperty));
    }

    private static Properties getPropertySection(final Properties configuration, final String prefix) {
        final Properties props = new Properties();
        configuration.stringPropertyNames().stream().filter(key -> key.startsWith(prefix))
            .forEach(key -> props.setProperty(key.substring(prefix.length()), configuration.getProperty(key)));
        return props;
    }

}
