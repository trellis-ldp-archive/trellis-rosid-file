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
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

import edu.amherst.acdc.trellis.api.MementoLink;
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

    private static Function<Triple, String> ntriplesString = triple ->
        join(" ", triple.getSubject().ntriplesString(), triple.getPredicate().ntriplesString(),
                triple.getObject().ntriplesString(), ".");

    private static void uncheckedWrite(final Writer writer, final String string) {
        try {
            writer.write(string);
        } catch (final IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    /**
     * Write new triples to a journaled resource file
     * @param directory the directory
     * @param add the triples to add
     * @param remove the triples to remove
     */
    public static void write(final File directory, final Stream<Triple> add, final Stream<Triple> remove) {
        final File journal = new File(directory, RESOURCE_JOURNAL);
        try (final BufferedWriter writer = newBufferedWriter(journal.toPath(), UTF_8, APPEND)) {
            remove.map(ntriplesString).forEach(ntriple -> {
                uncheckedWrite(writer, join(" ", "D", ntriple, lineSeparator()));
            });
            add.map(ntriplesString).forEach(ntriple -> {
                uncheckedWrite(writer, join(" ", "A", ntriple, lineSeparator()));
            });
        } catch (final IOException ex) {
            throw new UncheckedIOException(ex);
        }
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
}
