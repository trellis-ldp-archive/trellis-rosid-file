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

import static com.fasterxml.jackson.databind.SerializationFeature.WRITE_DATES_AS_TIMESTAMPS;
import static java.lang.System.lineSeparator;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.lines;
import static java.nio.file.Files.newBufferedWriter;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.nio.file.StandardOpenOption.WRITE;
import static java.time.Instant.now;
import static java.time.Instant.parse;
import static java.util.Collections.emptyIterator;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static java.util.Optional.ofNullable;
import static java.util.Spliterator.IMMUTABLE;
import static java.util.Spliterator.NONNULL;
import static java.util.Spliterator.ORDERED;
import static java.util.Spliterators.spliteratorUnknownSize;
import static java.util.stream.Stream.empty;
import static org.slf4j.LoggerFactory.getLogger;
import static org.trellisldp.rosid.file.Constants.MEMENTO_CACHE;
import static org.trellisldp.rosid.file.Constants.RESOURCE_CACHE;
import static org.trellisldp.rosid.file.Constants.RESOURCE_JOURNAL;
import static org.trellisldp.rosid.file.Constants.RESOURCE_QUADS;
import static org.trellisldp.rosid.file.FileUtils.stringToQuad;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Iterator;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.slf4j.Logger;

import org.trellisldp.api.Resource;
import org.trellisldp.api.VersionRange;
import org.trellisldp.rosid.common.ResourceData;

/**
 * An object that mediates access to the resource cache files.
 *
 * @author acoburn
 */
public class CachedResource extends AbstractFileResource {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final Logger LOGGER = getLogger(CachedResource.class);

    static {
        MAPPER.configure(WRITE_DATES_AS_TIMESTAMPS, false);
        MAPPER.registerModule(new JavaTimeModule());
    }

    /**
     * Create a File-based resource reader
     * @param directory the data storage directory
     * @param identifier the resource to retrieve
     * @param data the resource data
     */
    protected CachedResource(final File directory, final IRI identifier, final ResourceData data) {
        super(directory, identifier, data);
        LOGGER.debug("Creating a Cached Resource for {}", identifier.getIRIString());
    }

    /**
     * Retrieve a cached resource, if it exists
     * @param directory the directory
     * @param identifier the identifier
     * @return the resource
     */
    public static Optional<Resource> find(final File directory, final IRI identifier) {
        ResourceData data = null;
        if (isNull(directory)) {
            return Optional.empty();
        }
        try {
            data = MAPPER.readValue(new File(directory, RESOURCE_CACHE), ResourceData.class);
        } catch (final IOException ex) {
            LOGGER.warn("Error reading cached resource: {}", ex.getMessage());
        }
        return ofNullable(data).map(d -> new CachedResource(directory, identifier, d));
    }

    /**
     * Write the resource data into a file as JSON
     * @param directory the directory
     * @param identifier the resource identifier
     * @return true if the write operation succeeds
     */
    public static Boolean write(final File directory, final String identifier) {
        return nonNull(directory) && write(directory, rdf.createIRI(identifier));
    }

    /**
     * Write the resource data into a file as JSON
     * @param directory the directory
     * @param identifier the resource identifier
     * @return true if the write operation succeeds
     */
    public static Boolean write(final File directory, final IRI identifier) {
        return nonNull(directory) && write(directory, identifier, now());
    }

    /**
     * Write the resource data into a file as JSON
     * @param directory the directory
     * @param identifier the resource identifier
     * @param time the time
     * @return true if the write operation succeeds
     */
    public static Boolean write(final File directory, final String identifier, final Instant time) {
        return nonNull(directory) && write(directory, rdf.createIRI(identifier), time);
    }

    /**
     * Write the resource data into a file as JSON
     * @param directory the directory
     * @param identifier the resource identifier
     * @param time the time
     * @return true if the write operation succeeds
     */
    public static Boolean write(final File directory, final IRI identifier, final Instant time) {

        if (isNull(directory)) {
            return false;
        }

        // Write the JSON file
        LOGGER.debug("Writing JSON cache for {}", identifier.getIRIString());
        final Optional<ResourceData> data = VersionedResource.read(directory, identifier, time);
        try {
            if (data.isPresent()) {
                MAPPER.writeValue(new File(directory, RESOURCE_CACHE), data.get());
            } else {
                LOGGER.error("No resource data to cache for {}", identifier.getIRIString());
                return false;
            }
        } catch (final IOException ex) {
            LOGGER.error("Error writing resource metadata cache for {}: {}",
                    identifier.getIRIString(), ex.getMessage());
        }

        // Write the quads
        LOGGER.debug("Writing NQuads cache for {}", identifier.getIRIString());
        try (final BufferedWriter writer = newBufferedWriter(new File(directory, RESOURCE_QUADS).toPath(),
                    UTF_8, CREATE, WRITE, TRUNCATE_EXISTING)) {
            final File file = new File(directory, RESOURCE_JOURNAL);
            final Iterator<String> lineIter = RDFPatch.asStream(rdf, file, identifier, time)
                    .map(RDFPatch.quadToString).iterator();
            while (lineIter.hasNext()) {
                writer.write(lineIter.next() + lineSeparator());
            }
        } catch (final IOException ex) {
            LOGGER.error("Error writing resource cache for {}: {}", identifier.getIRIString(), ex.getMessage());
            return false;
        }

        // Write the mementos
        LOGGER.debug("Writing the Mementos for {}", identifier.getIRIString());
        try (final BufferedWriter writer = newBufferedWriter(new File(directory, MEMENTO_CACHE).toPath(),
                    UTF_8, CREATE, WRITE, TRUNCATE_EXISTING)) {
            final File file = new File(directory, RESOURCE_JOURNAL);
            final Iterator<VersionRange> iter = RDFPatch.asTimeMap(file).iterator();
            if (iter.hasNext()) {
                final VersionRange range = iter.next();
                writer.write(range.getFrom() + lineSeparator());
                writer.write(range.getUntil() + lineSeparator());
            }
            while (iter.hasNext()) {
                writer.write(iter.next().getUntil() + lineSeparator());
            }
        } catch (final IOException ex) {
            LOGGER.error("Error writing memento cache for {}: {}", identifier.getIRIString(), ex.getMessage());
            return false;
        }

        return true;
    }

    @Override
    public Stream<VersionRange> getMementos() {
        return StreamSupport.stream(spliteratorUnknownSize(new MementoReader(new File(directory, MEMENTO_CACHE)),
                    IMMUTABLE | NONNULL | ORDERED), false);
    }

    @Override
    public Stream<Quad> stream() {
        return Optional.of(new File(directory, RESOURCE_QUADS)).filter(File::exists).map(File::toPath)
            .map(uncheckedLines).orElse(empty())
            .map(line -> stringToQuad(rdf, line)).filter(Optional::isPresent).map(Optional::get);
    }

    private Function<Path, Stream<String>> uncheckedLines = path -> {
        try {
            return lines(path);
        } catch (final IOException ex) {
            LOGGER.warn("Could not read file at {}: {}", path.toString(), ex.getMessage());
        }
        return empty();
    };

    /**
     * A class for reading a file of change times
     */
    private static class MementoReader implements Iterator<VersionRange> {
        private final Iterator<String> dateLines;
        private Instant from = null;

        /**
         * Create a new MementoReader
         * @param file the file
         */
        public MementoReader(final File file) {
            dateLines = getLines(file.toPath());
            if (dateLines.hasNext()) {
                from = parse(dateLines.next());
            }
        }

        private static Iterator<String> getLines(final Path path) {
            try {
                return lines(path).iterator();
            } catch (final IOException ex) {
                LOGGER.warn("Could not read Memento cache: {}", ex.getMessage());
            }
            return emptyIterator();
        }

        @Override
        public boolean hasNext() {
            return dateLines.hasNext();
        }

        @Override
        public VersionRange next() {
            final String line = dateLines.next();
            if (nonNull(line)) {
                final Instant until = parse(line);
                final VersionRange range = new VersionRange(from, until);
                from = until;
                return range;
            }
            return null;
        }
    }
}
