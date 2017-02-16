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

import static java.io.File.separator;
import static java.nio.file.Files.readAttributes;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static edu.amherst.acdc.trellis.rosid.Constants.RESOURCE_CACHE;
import static edu.amherst.acdc.trellis.rosid.Constants.RESOURCE_JOURNAL;
import static edu.amherst.acdc.trellis.rosid.FileUtils.asPath;
import static edu.amherst.acdc.trellis.rosid.FileUtils.partition;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

import org.apache.commons.rdf.api.Graph;
import org.apache.commons.rdf.api.IRI;
import org.slf4j.Logger;

import edu.amherst.acdc.trellis.api.Resource;
import edu.amherst.acdc.trellis.spi.EventService;
import edu.amherst.acdc.trellis.spi.ResourceService;
import edu.amherst.acdc.trellis.spi.Session;

/**
 * @author acoburn
 */
public class FileRepositoryService implements ResourceService {

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
        evtSvc = svc;
    }

    @Override
    public void unbind(final EventService svc) {
        if (evtSvc == svc) {
            evtSvc = null;
        }
    }

    @Override
    public Boolean exists(final Session session, final IRI identifier) {
        // TODO -- this naively ignores the session (e.g. batch ops)
        final File resource = new File(directory, partition(asPath(identifier)) + separator + RESOURCE_CACHE);
        return resource.exists();
    }

    @Override
    public Boolean exists(final Session session, final IRI identifier, final Instant time) {
        // TODO -- this naively ignores the session (e.g. batch ops)
        final File resource = new File(directory, partition(asPath(identifier)) + separator + RESOURCE_JOURNAL);
        try {
            return resource.exists() && !readAttributes(resource.toPath(), BasicFileAttributes.class).creationTime()
                .toInstant().isAfter(time);
        } catch (final IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    @Override
    public Optional<Resource> find(final Session session, final IRI identifier) {
        return of(new File(directory, partition(asPath(identifier)))).filter(File::exists)
            .flatMap(dir -> of(new CachedResource(dir, identifier)));
    }

    @Override
    public Optional<Resource> find(final Session session, final IRI identifier, final Instant time) {
        return of(new File(directory, partition(asPath(identifier)))).filter(File::exists)
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
}
