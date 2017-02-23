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

import static edu.amherst.acdc.trellis.rosid.FileUtils.stringToQuad;
import static java.lang.String.join;
import static java.lang.System.lineSeparator;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.lines;
import static java.nio.file.Files.newBufferedWriter;
import static java.nio.file.StandardOpenOption.APPEND;
import static java.time.Instant.parse;
import static java.util.Objects.nonNull;
import static java.util.stream.StreamSupport.stream;

import edu.amherst.acdc.trellis.api.VersionRange;
import edu.amherst.acdc.trellis.vocabulary.DC;
import edu.amherst.acdc.trellis.vocabulary.Fedora;
import edu.amherst.acdc.trellis.vocabulary.Trellis;
import edu.amherst.acdc.trellis.vocabulary.XSD;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.time.Instant;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import org.apache.commons.io.input.ReversedLinesFileReader;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.apache.commons.rdf.api.RDF;

/**
 * @author acoburn
 */
final class RDFPatch {

    private static final String COMMENT_DELIM = " # ";
    private static final String BEGIN = "BEGIN" + COMMENT_DELIM;
    private static final String END = "END" + COMMENT_DELIM;

    /**
     * Read the triples from the journal that existed up to (and including) the specified time
     * @param rdf the rdf object
     * @param file the file
     * @param identifier the identifier
     * @param time the time
     * @return a stream of RDF triples
     */
    public static Stream<Quad> asStream(final RDF rdf, final File file, final IRI identifier, final Instant time) {
        return stream(new StreamReader(rdf, file, identifier, time), false);
    }

    /**
     * Retrieve time values for the history of the resource
     * @param file the file
     * @return a stream of VersionRange objects
     */
    public static Stream<VersionRange> asTimeMap(final File file) {
        return stream(new TimeMapReader(file), false);
    }

    /**
     * Delete RDF Patch statements from the specified file
     * @param file the file
     * @param delete the quads to delete
     * @param add the quads to add
     * @param time the time
     * @param throws IOException if the writer encounters an error writing to the file
     */
    public static void write(final File file, final Stream<Quad> delete, final Stream<Quad> add, final Instant time)
            throws IOException {
        try (final BufferedWriter writer = newBufferedWriter(file.toPath(), UTF_8, APPEND)) {
            writer.write(BEGIN + time + lineSeparator());
            delete.map(quadToString).forEach(quad -> uncheckedWrite(writer, "D " + quad));
            add.map(quadToString).forEach(quad -> uncheckedWrite(writer, "A " + quad));
            writer.write(END + time + lineSeparator());
        } catch (final IOException ex) {
            throw ex;
        }
    }

    private static final Function<Quad, String> quadToString = quad ->
        join(" ", quad.getSubject().ntriplesString(), quad.getPredicate().ntriplesString(),
                quad.getObject().ntriplesString(),
                quad.getGraphName().orElse(Trellis.PreferUserManaged).ntriplesString(), ".");

    private static void uncheckedWrite(final Writer writer, final String line) {
        try {
            writer.write(line + lineSeparator());
        } catch (final IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    /**
     * A class for reading an RDF Patch file into a VersionRange Spliterator
     */
    private static class TimeMapReader implements Spliterator<VersionRange> {
        private final Iterator<String> dateLines;

        private Instant from = null;

        /**
         * Create a time map reader
         * @param file the file
         */
        public TimeMapReader(final File file) {
            try {
                dateLines = lines(file.toPath()).filter(line -> line.startsWith(BEGIN)).iterator();
            } catch (final IOException ex) {
                throw new UncheckedIOException(ex);
            }
        }

        @Override
        public void forEachRemaining(final Consumer<? super VersionRange> action) {
            while (dateLines.hasNext()) {
                from = emit(action, dateLines.next());
            }
        }

        @Override
        public boolean tryAdvance(final Consumer<? super VersionRange> action) {
            if (dateLines.hasNext()) {
                from = emit(action, dateLines.next());
                return true;
            }
            return false;
        }

        @Override
        public Spliterator<VersionRange> trySplit() {
            return null;
        }

        @Override
        public long estimateSize() {
            return Long.MAX_VALUE;
        }

        @Override
        public int characteristics() {
            return ORDERED | NONNULL | IMMUTABLE;
        }

        private Instant emit(final Consumer<? super VersionRange> action, final String line) {
            final Instant time = parse(line.split(COMMENT_DELIM)[1]);
            if (nonNull(from)) {
                action.accept(new VersionRange(from, time));
            }
            return time;
        }
    }

    /**
     * A class for reading an RDFPatch file into a Quad Spliterator.
     */
    private static class StreamReader implements Spliterator<Quad> {

        private final Set<Quad> deleted = new HashSet<>();
        private final ReversedLinesFileReader reader;
        private final Instant time;
        private final RDF rdf;
        private final IRI identifier;

        private Boolean inRegion = false;
        private Boolean hasModified = false;
        private Instant modified;

        /**
         * Create a spliterator that reads a file line-by-line in reverse
         * @param rdf the RDF object
         * @param file the file
         * @param identifier the identifier
         * @param time the time
         */
        public StreamReader(final RDF rdf, final File file, final IRI identifier, final Instant time) {
            this.rdf = rdf;
            this.time = time;
            this.identifier = identifier;
            this.modified = time;
            try {
                this.reader = new ReversedLinesFileReader(file, UTF_8);
            } catch (final IOException ex) {
                throw new UncheckedIOException(ex);
            }
        }

        @Override
        public void forEachRemaining(final Consumer<? super Quad> action) {
            try {
                for (String line = reader.readLine(); nonNull(line); line = reader.readLine()) {
                    if (line.startsWith(END)) {
                        final Instant moment = parse(line.split(COMMENT_DELIM, 2)[1]);
                        if (!time.isBefore(moment)) {
                            modified = moment;
                            inRegion = true;
                        }
                    } else if (inRegion && (line.startsWith("A ") || line.startsWith("D "))) {
                        final String[] parts = line.split(" ", 2);
                        stringToQuad(rdf, parts[1]).ifPresent(quad -> {
                            if (!hasModified && !quad.getGraphName().filter(Fedora.PreferInboundReferences::equals)
                                    .isPresent()) {
                                action.accept(rdf.createQuad(Trellis.PreferServerManaged, identifier, DC.modified,
                                            rdf.createLiteral(modified.toString(), XSD.dateTime)));
                                hasModified = true;
                            }
                            if (parts[0].equals("D")) {
                                deleted.add(quad);
                            } else if (parts[0].equals("A") && !deleted.contains(quad)) {
                                action.accept((Quad) quad);
                            }
                        });
                    }
                }
            } catch (final IOException ex) {
                throw new UncheckedIOException(ex);
            }
        }

        @Override
        public boolean tryAdvance(final Consumer<? super Quad> action) {
            try {
                final String line = reader.readLine();
                if (nonNull(line)) {
                    if (line.startsWith(END)) {
                        final Instant moment = parse(line.split(COMMENT_DELIM, 2)[1]);
                        if (!time.isBefore(moment)) {
                            modified = moment;
                            inRegion = true;
                        }
                    } else if (inRegion && (line.startsWith("A ") || line.startsWith("D "))) {
                        final String[] parts = line.split(" ", 2);
                        stringToQuad(rdf, parts[1]).ifPresent(quad -> {
                            if (!hasModified && !quad.getGraphName().filter(Fedora.PreferInboundReferences::equals)
                                    .isPresent()) {
                                action.accept(rdf.createQuad(Trellis.PreferServerManaged, identifier, DC.modified,
                                            rdf.createLiteral(modified.toString(), XSD.dateTime)));
                                hasModified = true;
                            }
                            if (parts[0].equals("D")) {
                                deleted.add(quad);
                            } else if (parts[0].equals("A") && !deleted.contains(quad)) {
                                action.accept((Quad) quad);
                            }
                        });
                    }
                }
                return false;
            } catch (final IOException ex) {
                throw new UncheckedIOException(ex);
            }
        }

        @Override
        public Spliterator<Quad> trySplit() {
            return null;
        }

        @Override
        public long estimateSize() {
            return Long.MAX_VALUE;
        }

        @Override
        public int characteristics() {
            return ORDERED | NONNULL | IMMUTABLE;
        }
    }

    private RDFPatch() {
        // prevent instantiation
    }
}
