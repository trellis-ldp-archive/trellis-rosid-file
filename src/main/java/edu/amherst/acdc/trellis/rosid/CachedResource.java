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

import static java.nio.file.Files.lines;
import static java.util.Optional.of;
import static java.util.stream.Stream.empty;
import static com.fasterxml.jackson.databind.SerializationFeature.WRITE_DATES_AS_TIMESTAMPS;
import static edu.amherst.acdc.trellis.rosid.Constants.RESOURCE_CACHE;
import static edu.amherst.acdc.trellis.rosid.Constants.RESOURCE_QUADS;
import static edu.amherst.acdc.trellis.rosid.FileUtils.stringToQuad;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import edu.amherst.acdc.trellis.api.Resource;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.apache.commons.rdf.api.Triple;
import org.slf4j.Logger;

/**
 * An object that mediates access to the resource cache files.
 *
 * @author acoburn
 */
class CachedResource extends AbstractFileResource {

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
     */
    protected CachedResource(final File directory, final IRI identifier, final ResourceData data) {
        super(directory, identifier, data);
    }

    /**
     * Retrieve a cached resource, if it exists
     * @param directory the directory
     * @param identifier the identifier
     * @return the resource
     */
    public static Optional<Resource> find(final File directory, final IRI identifier) {
        try {
            final ResourceData data = MAPPER.readValue(new File(directory, RESOURCE_CACHE), ResourceData.class);
            return of(new CachedResource(directory, identifier, data));
        } catch (final IOException ex) {
            LOGGER.warn("Error reading cached resource: {}", ex.getMessage());
        }
        return Optional.empty();
    }

    public static void write(final File directory, final ResourceData json) {
        try {
            MAPPER.writeValue(new File(directory, RESOURCE_CACHE), json);
        } catch (final IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    @Override
    public <T extends Resource.TripleCategory> Stream<Triple> stream(final Collection<T> category) {
        return Optional.of(new File(directory, RESOURCE_QUADS)).filter(File::exists).map(File::toPath)
            .map(uncheckedLines).orElse(empty()).map(line -> stringToQuad(rdf, line)).filter(Optional::isPresent)
            .map(Optional::get).filter(quad -> quad.getGraphName().isPresent() &&
                    category.contains(categorymap.get(quad.getGraphName().get()))).map(Quad::asTriple);
    }

    private Function<Path, Stream<String>> uncheckedLines = path -> {
        try {
            return lines(path);
        } catch (final IOException ex) {
            throw new UncheckedIOException(ex);
        }
    };
}
