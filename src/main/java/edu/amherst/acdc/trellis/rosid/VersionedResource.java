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

import static java.lang.String.join;
import static java.lang.System.lineSeparator;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.newBufferedWriter;
import static java.nio.file.StandardOpenOption.APPEND;
import static edu.amherst.acdc.trellis.rosid.Constants.RESOURCE_JOURNAL;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.time.Instant;
import java.util.Optional;
import java.util.stream.Stream;

import edu.amherst.acdc.trellis.api.MementoLink;
import edu.amherst.acdc.trellis.vocabulary.DC;
import edu.amherst.acdc.trellis.vocabulary.XSD;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Triple;

/**
 * An object that mediates access to the resource version files.
 *
 * @author acoburn
 */
class VersionedResource extends AbstractFileResource {

    /**
     * Create a File-based versioned resource
     * @param directory the directory
     * @param identifier the resource identifier
     */
    public VersionedResource(final File directory, final IRI identifier) {
        super(directory, identifier);

        // TODO -- read from journal to set this.data

    }

    /**
     * Write new triples to a journaled resource file
     * @param directory the directory
     * @param statements the RDF-Patch statements
     * @param identifier the identifier
     * @param time the time
     * @param agent the agent
     */
    public static void write(final File directory, final Stream<String> statements, final IRI identifier,
            final Instant time, final IRI agent) {
        final File journal = new File(directory, RESOURCE_JOURNAL);
        try (final BufferedWriter writer = newBufferedWriter(journal.toPath(), UTF_8, APPEND)) {
            final String created = join(" ", identifier.ntriplesString(), DC.created.ntriplesString(),
                    rdf.createLiteral(time.toString(), XSD.dateTime).ntriplesString(), ".");
            final String creator = join(" ", identifier.ntriplesString(), DC.creator.ntriplesString(),
                    agent.ntriplesString(), ".");

            writer.write("BEGIN # " + created + lineSeparator());
            writer.write("# " + creator + lineSeparator());
            statements.forEach(statement -> {
                uncheckedWrite(writer, statement + lineSeparator());
            });
            writer.write("END # " + created + lineSeparator());
        } catch (final IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    private static ResourceData read(final File directory, final Instant time) {
        final ResourceData rd = new ResourceData();
        return rd;
    }

    private static ResourceData read(final File directory) {
        final ResourceData rd = new ResourceData();
        return rd;
    }

    @Override
    public Boolean isMemento() {
        return true;
    }

    @Override
    public Optional<IRI> getTimeMap() {
        // TODO -- getOriginal() + "?format=timemap"
        return Optional.empty();
    }

    @Override
    public Stream<MementoLink> getMementos() {
        // TODO -- get from storage layer
        return Stream.empty();
    }

    @Override
    public Stream<IRI> getContains() {
        // TODO -- read from the data storage
        return Stream.empty();
    }

    protected Stream<Triple> getMembershipTriples() {
        // TODO -- read from data storage
        return Stream.empty();
    }

    protected Stream<Triple> getInboundTriples() {
        // TODO -- read from data storage
        return Stream.empty();
    }

    protected Stream<Triple> getUserTriples() {
        // TODO -- read from data storage
        return Stream.empty();
    }

    protected Stream<Triple> getAuditTriples() {
        // TODO -- read from data storage
        return Stream.empty();
    }

    private static void uncheckedWrite(final Writer writer, final String string) {
        try {
            writer.write(string);
        } catch (final IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

}
