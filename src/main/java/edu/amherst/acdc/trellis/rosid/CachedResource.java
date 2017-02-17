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
import static edu.amherst.acdc.trellis.api.Resource.TripleContext.FEDORA_INBOUND_REFERENCES;
import static edu.amherst.acdc.trellis.api.Resource.TripleContext.LDP_CONTAINMENT;
import static edu.amherst.acdc.trellis.api.Resource.TripleContext.LDP_MEMBERSHIP;
import static edu.amherst.acdc.trellis.api.Resource.TripleContext.TRELLIS_AUDIT;
import static edu.amherst.acdc.trellis.api.Resource.TripleContext.USER_MANAGED;
import static edu.amherst.acdc.trellis.rosid.Constants.AUDIT_CACHE;
import static edu.amherst.acdc.trellis.rosid.Constants.CONTAINMENT_CACHE;
import static edu.amherst.acdc.trellis.rosid.Constants.INBOUND_CACHE;
import static edu.amherst.acdc.trellis.rosid.Constants.MEMBERSHIP_CACHE;
import static edu.amherst.acdc.trellis.rosid.Constants.RESOURCE_CACHE;
import static edu.amherst.acdc.trellis.rosid.Constants.USER_CACHE;
import static org.apache.jena.riot.Lang.NTRIPLES;
import static org.apache.commons.rdf.jena.JenaRDF.asTriple;
import static org.apache.jena.riot.system.StreamRDFLib.sinkTriples;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import edu.amherst.acdc.trellis.api.Resource;
import edu.amherst.acdc.trellis.vocabulary.LDP;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Triple;
import org.apache.jena.atlas.lib.SinkToCollection;
import org.apache.jena.riot.RDFParserRegistry;
import org.apache.jena.riot.ReaderRIOT;
import org.slf4j.Logger;

/**
 * An object that mediates access to the resource cache files.
 *
 * @author acoburn
 */
class CachedResource extends AbstractFileResource {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final ReaderRIOT READER = RDFParserRegistry.getFactory(NTRIPLES).create(NTRIPLES);

    private static final Logger LOGGER = getLogger(CachedResource.class);

    protected final Map<Resource.TripleContext, Supplier<Stream<Triple>>> fnmap = new HashMap<>();

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

        // define mappings for triple contexts
        fnmap.put(LDP_CONTAINMENT, this::getContainmentTriples);
        fnmap.put(LDP_MEMBERSHIP, this::getMembershipTriples);
        fnmap.put(FEDORA_INBOUND_REFERENCES, this::getInboundTriples);
        fnmap.put(USER_MANAGED, this::getUserTriples);
        fnmap.put(TRELLIS_AUDIT, this::getAuditTriples);
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
    public Stream<IRI> getContains() {
        return of(new File(directory, CONTAINMENT_CACHE)).filter(File::exists).map(File::toPath).map(uncheckedLines)
            .orElse(empty()).map(rdf::createIRI);
    }

    @Override
    public <T extends Resource.TripleCategory> Stream<Triple> stream(final Collection<T> category) {
        return category.stream().filter(fnmap::containsKey).map(fnmap::get).flatMap(Supplier::get);
    }


    private Stream<Triple> getMembershipTriples() {
        return of(new File(directory, MEMBERSHIP_CACHE)).filter(File::exists).map(File::toPath).map(uncheckedLines)
            .orElse(empty()).flatMap(readNTriple);
    }

    private Stream<Triple> getInboundTriples() {
        return of(new File(directory, INBOUND_CACHE)).filter(File::exists).map(File::toPath).map(uncheckedLines)
            .orElse(empty()).flatMap(readNTriple);
    }

    private Stream<Triple> getUserTriples() {
        return of(new File(directory, USER_CACHE)).filter(File::exists).map(File::toPath).map(uncheckedLines)
            .orElse(empty()).flatMap(readNTriple);
    }

    private Stream<Triple> getAuditTriples() {
        return of(new File(directory, AUDIT_CACHE)).filter(File::exists).map(File::toPath).map(uncheckedLines)
            .orElse(empty()).flatMap(readNTriple);
    }

    private Stream<Triple> getContainmentTriples() {
        return getContains().map(uri -> rdf.createTriple(getIdentifier(), LDP.contains, uri));
    }

    private Function<Path, Stream<String>> uncheckedLines = path -> {
        try {
            return lines(path);
        } catch (final IOException ex) {
            throw new UncheckedIOException(ex);
        }
    };

    private static Function<String, Stream<Triple>> readNTriple = line -> {
        final List<org.apache.jena.graph.Triple> c = new ArrayList<>();
        READER.read(new StringReader(line), null, NTRIPLES.getContentType(),
                sinkTriples(new SinkToCollection<>(c)), null);
        return c.stream().map(triple -> asTriple(rdf, triple));
    };

}
