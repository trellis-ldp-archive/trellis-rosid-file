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

import static java.net.URI.create;
import static java.time.Instant.now;
import static java.util.Objects.isNull;
import static java.util.Objects.requireNonNull;
import static java.util.Optional.ofNullable;
import static java.util.stream.Stream.empty;
import static java.util.stream.Stream.of;
import static org.slf4j.LoggerFactory.getLogger;
import static org.trellisldp.rosid.file.Constants.RESOURCE_CACHE;
import static org.trellisldp.rosid.file.Constants.RESOURCE_JOURNAL;
import static org.trellisldp.rosid.file.FileUtils.getPropertySection;
import static org.trellisldp.rosid.file.FileUtils.getStorageConfig;
import static org.trellisldp.rosid.file.FileUtils.resourceDirectory;

import java.io.File;
import java.io.IOException;
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

    private static final String STORAGE_PREFIX = "trellis.storage.";

    private final Map<String, String> resourceConfig;

    /**
     * Create a File-based repository service
     * @param configuration the configuration
     * @param curator the curator framework
     * @param producer the kafka producer
     * @param notifications the notification service
     * @throws IOException if the directory is not writable
     */
    public FileResourceService(final Properties configuration, final CuratorFramework curator,
            final Producer<String, Dataset> producer, final EventService notifications) throws IOException {
        super(producer, curator, notifications);
        requireNonNull(configuration, "configuration may not be null!");
        this.resourceConfig = getStorageConfig(getPropertySection(configuration, STORAGE_PREFIX), ".resources");

        init();
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
        return RDFPatch.write(new File(dir, RESOURCE_JOURNAL), remove, add, time) &&
            CachedResource.write(dir, identifier);
    }

    private void init() throws IOException {
        for (final Map.Entry<String, String> storage : resourceConfig.entrySet()) {
            final File data = storage.getValue().startsWith("file:") ?
                 new File(create(storage.getValue())) : new File(storage.getValue());
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
                        rdf.createQuad(Trellis.PreferAudit, skolem, PROV.wasAssociatedWith,
                            Trellis.RepositoryAdministrator),
                        rdf.createQuad(Trellis.PreferAudit, skolem, PROV.generatedAtTime,
                            rdf.createLiteral(time.toString(), XSD.dateTime)));
                RDFPatch.write(rootData, empty(), quads, now());
                CachedResource.write(root, identifier);
            }
        }
    }
}
