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
import static java.io.File.separator;
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
import java.io.UncheckedIOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

import org.apache.commons.rdf.api.Graph;
import org.apache.commons.rdf.api.IRI;
import org.slf4j.Logger;

/**
 * @author acoburn
 */
public class FileRepositoryService implements ResourceService, AutoCloseable {

    private static final Logger LOGGER = getLogger(FileRepositoryService.class);

    private EventService evtSvc;
    private File directory;

    /**
     * Create a File-based repository service
     * @param directory the data directory
     */
    public FileRepositoryService(final String directory) {
        this(new File(directory));
    }

    /**
     * Create a File-based repository service
     * @param directory the data directory
     */
    public FileRepositoryService(final File directory) {
        requireNonNull(directory, "directory may not be null!");

        if (!directory.exists()) {
            directory.mkdirs();
        }
        if (!directory.canWrite()) {
            throw new UncheckedIOException(new IOException("Cannot write to " + directory.getAbsolutePath()));
        }
        this.directory = directory;
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
    public Boolean exists(final Session session, final IRI identifier) {
        // TODO -- this naively ignores the session (e.g. batch ops)
        final File resource = new File(directory, partition(identifier) + separator + RESOURCE_CACHE);
        return resource.exists();
    }

    @Override
    public Boolean exists(final Session session, final IRI identifier, final Instant time) {
        // TODO -- this naively ignores the session (e.g. batch ops)
        return VersionedResource.read(directory, identifier, time).isPresent();
    }

    @Override
    public Optional<Resource> find(final Session session, final IRI identifier) {
        // TODO -- this naively ignores the session (e.g. batch ops)
        return of(new File(directory, partition(identifier))).filter(File::exists)
            .flatMap(dir -> CachedResource.find(dir, identifier));
    }

    @Override
    public Optional<Resource> find(final Session session, final IRI identifier, final Instant time) {
        // TODO -- this naively ignores the session (e.g. batch ops)
        return of(new File(directory, partition(identifier))).filter(File::exists)
            .flatMap(dir -> VersionedResource.find(dir, identifier, time));
    }

    @Override
    public Resource create(final Session session, final IRI identifier, final IRI type) {
        // TODO
        return null;
    }

    @Override
    public void update(final Session session, final IRI identifier, final Graph graph) {
        // TODO
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
        // TODO
        // close any zk/kafka connections
    }
}
