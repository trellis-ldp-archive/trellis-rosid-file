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
import static edu.amherst.acdc.trellis.rosid.Constants.AUDIT_CACHE;
import static edu.amherst.acdc.trellis.rosid.Constants.CONTAINMENT_CACHE;
import static edu.amherst.acdc.trellis.rosid.Constants.INBOUND_CACHE;
import static edu.amherst.acdc.trellis.rosid.Constants.MEMBERSHIP_CACHE;
import static edu.amherst.acdc.trellis.rosid.Constants.RESOURCE_CACHE;
import static edu.amherst.acdc.trellis.rosid.Constants.USER_CACHE;
import static org.apache.commons.rdf.jena.JenaRDF.asTriple;
import static org.apache.jena.riot.Lang.NTRIPLES;
import static org.apache.jena.riot.system.StreamRDFLib.sinkTriples;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import edu.amherst.acdc.trellis.api.MementoLink;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Triple;
import org.apache.jena.atlas.lib.SinkToCollection;
import org.apache.jena.riot.RDFParserRegistry;
import org.apache.jena.riot.ReaderRIOT;

/**
 * An object that mediates access to the resource cache files.
 *
 * @author acoburn
 */
class CachedResource extends AbstractFileResource {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final ReaderRIOT READER = RDFParserRegistry.getFactory(NTRIPLES).create(NTRIPLES);

    static {
        MAPPER.configure(WRITE_DATES_AS_TIMESTAMPS, false);
        MAPPER.registerModule(new JavaTimeModule());
    }

    /**
     * Create a File-based resource reader
     * @param directory the data storage directory
     * @param identifier the resource to retrieve
     */
    public CachedResource(final File directory, final IRI identifier) {
        super(directory, identifier);

        try {
            this.data = MAPPER.readValue(new File(directory, RESOURCE_CACHE), ResourceData.class);
        } catch (final IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    public static void write(final File directory, final ResourceData json) {
        try {
            MAPPER.writeValue(new File(directory, RESOURCE_CACHE), json);
        } catch (final IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    @Override
    public Optional<IRI> getTimeMap() {
        // TODO -- getOriginal() + "?format=timemap"
        return Optional.empty();
    }

    @Override
    public Stream<MementoLink> getMementos() {
        // TODO -- get from storage layer (memento_cache)
        return Stream.empty();
    }

    @Override
    public Stream<IRI> getContains() {
        return of(new File(directory, CONTAINMENT_CACHE)).filter(File::exists).map(File::toPath).map(uncheckedLines)
            .orElse(empty()).map(rdf::createIRI);
    }

    @Override
    protected Stream<Triple> getMembershipTriples() {
        return of(new File(directory, MEMBERSHIP_CACHE)).filter(File::exists).map(File::toPath).map(uncheckedLines)
            .orElse(empty()).flatMap(readNTriple);
    }

    @Override
    protected Stream<Triple> getInboundTriples() {
        return of(new File(directory, INBOUND_CACHE)).filter(File::exists).map(File::toPath).map(uncheckedLines)
            .orElse(empty()).flatMap(readNTriple);
    }

    @Override
    protected Stream<Triple> getUserTriples() {
        return of(new File(directory, USER_CACHE)).filter(File::exists).map(File::toPath).map(uncheckedLines)
            .orElse(empty()).flatMap(readNTriple);
    }

    @Override
    protected Stream<Triple> getAuditTriples() {
        return of(new File(directory, AUDIT_CACHE)).filter(File::exists).map(File::toPath).map(uncheckedLines)
            .orElse(empty()).flatMap(readNTriple);
    }

    private Function<Path, Stream<String>> uncheckedLines = path -> {
        try {
            return lines(path);
        } catch (final IOException ex) {
            throw new UncheckedIOException(ex);
        }
    };

    private Function<String, Stream<Triple>> readNTriple = line -> {
        final List<org.apache.jena.graph.Triple> c = new ArrayList<>();
        READER.read(new StringReader(line), identifier.getIRIString(), NTRIPLES.getContentType(),
                sinkTriples(new SinkToCollection<>(c)), null);
        return c.stream().map(triple -> asTriple(rdf, triple));
    };
}
