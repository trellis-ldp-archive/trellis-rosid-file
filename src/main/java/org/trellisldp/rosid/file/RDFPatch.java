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

import static java.lang.String.join;
import static java.lang.System.lineSeparator;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.lines;
import static java.nio.file.Files.newBufferedWriter;
import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.time.Instant.parse;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static java.util.Spliterator.IMMUTABLE;
import static java.util.Spliterator.NONNULL;
import static java.util.Spliterator.ORDERED;
import static java.util.Spliterators.spliteratorUnknownSize;
import static java.util.stream.StreamSupport.stream;
import static org.slf4j.LoggerFactory.getLogger;
import static org.trellisldp.rosid.file.FileUtils.stringToQuad;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import org.apache.commons.io.input.ReversedLinesFileReader;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.apache.commons.rdf.api.RDF;
import org.slf4j.Logger;
import org.trellisldp.api.VersionRange;
import org.trellisldp.vocabulary.DC;
import org.trellisldp.vocabulary.Trellis;
import org.trellisldp.vocabulary.XSD;

/**
 * @author acoburn
 */
final class RDFPatch {

    private static final Logger LOGGER = getLogger(RDFPatch.class);

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
        final StreamReader reader = new StreamReader(rdf, file, identifier, time);
        return stream(spliteratorUnknownSize(reader, IMMUTABLE | NONNULL | ORDERED), false).onClose(reader::close);
    }

    /**
     * Retrieve time values for the history of the resource
     * @param file the file
     * @return a list of VersionRange objects
     */
    public static List<VersionRange> asTimeMap(final File file) {
        final List<VersionRange> ranges = new ArrayList<>();
        try (final TimeMapReader reader = new TimeMapReader(file)) {
            reader.forEachRemaining(ranges::add);
        }
        return unmodifiableList(ranges);
    }

    /**
     * Write RDF Patch statements to the specified file
     * @param file the file
     * @param delete the quads to delete
     * @param add the quads to add
     * @param time the time
     * @return true if the write succeeds; false otherwise
     */
    public static Boolean write(final File file, final Stream<? extends Quad> delete, final Stream<? extends Quad> add,
            final Instant time) {
        try (final BufferedWriter writer = newBufferedWriter(file.toPath(), UTF_8, CREATE, APPEND)) {
            writer.write(BEGIN + time + lineSeparator());
            final Iterator<String> delIter = delete.map(quadToString).iterator();
            while (delIter.hasNext()) {
                writer.write("D " + delIter.next() + lineSeparator());
            }
            final Iterator<String> addIter = add.map(quadToString).iterator();
            while (addIter.hasNext()) {
                writer.write("A " + addIter.next() + lineSeparator());
            }
            writer.write(END + time + lineSeparator());
        } catch (final IOException ex) {
            LOGGER.error("Error writing data to resource {}: {}", file, ex.getMessage());
            return false;
        }
        return true;
    }

    public static final Function<Quad, String> quadToString = quad ->
        join(" ",
                quad.getSubject().ntriplesString(), quad.getPredicate().ntriplesString(),
                quad.getObject().ntriplesString(),
                quad.getGraphName().orElse(Trellis.PreferUserManaged).ntriplesString(), ".");

    /**
     * A class for reading an RDF Patch file into a VersionRange Iterator
     */
    static class TimeMapReader implements Iterator<VersionRange>, AutoCloseable {
        private final Stream<String> lineStream;
        private final Iterator<String> allLines;
        private Instant from = null;
        private Boolean hasUserTriples = false;
        private VersionRange buffer = null;

        /**
         * Create a time map reader
         * @param file the file
         */
        public TimeMapReader(final File file) {
            try {
                lineStream = lines(file.toPath());
            } catch (final IOException ex) {
                throw new UncheckedIOException(ex);
            }
            allLines = lineStream.iterator();
            tryAdvance();
        }

        @Override
        public boolean hasNext() {
            return nonNull(buffer);
        }

        @Override
        public VersionRange next() {
            final VersionRange range = buffer;
            tryAdvance();
            if (nonNull(range)) {
                return range;
            }
            throw new NoSuchElementException();
        }

        @Override
        public void close() {
            lineStream.close();
        }

        private void tryAdvance() {
            while (allLines.hasNext()) {
                final String line = allLines.next();
                if (line.startsWith(BEGIN)) {
                    hasUserTriples = false;
                } else if (line.endsWith(Trellis.PreferUserManaged + " .") ||
                        line.endsWith(Trellis.PreferServerManaged + " .")) {
                    hasUserTriples = true;
                } else if (line.startsWith(END) && hasUserTriples) {
                    final Instant time = parse(line.split(COMMENT_DELIM)[1]);
                    if (nonNull(from)) {
                        if (time.isAfter(from)) {
                            buffer = new VersionRange(from, time);
                            from = time;
                        }
                        return;
                    }
                    from = time;
                }
            }
            buffer = null;
        }
    }

    /**
     * A class for reading an RDFPatch file into a Quad Iterator.
     */
    static class StreamReader implements Iterator<Quad>, AutoCloseable {

        private final Set<Quad> deleted = new HashSet<>();
        private final ReversedLinesFileReader reader;
        private final Instant time;
        private final RDF rdf;
        private final IRI identifier;

        private Boolean inRegion = false;
        private Boolean hasModified = false;
        private Quad buffer = null;
        private String line = null;

        /**
         * Create an iterator that reads a file line-by-line in reverse
         * @param rdf the RDF object
         * @param file the file
         * @param identifier the identifier
         * @param time the time
         */
        public StreamReader(final RDF rdf, final File file, final IRI identifier, final Instant time) {
            this.rdf = rdf;
            this.time = time;
            this.identifier = identifier;
            try {
                this.reader = new ReversedLinesFileReader(file, UTF_8);
                this.line = reader.readLine();
            } catch (final IOException ex) {
                throw new UncheckedIOException(ex);
            }
            tryAdvance();
        }

        @Override
        public boolean hasNext() {
            return nonNull(buffer);
        }

        @Override
        public Quad next() {
            final Quad quad = buffer;
            tryAdvance();
            if (nonNull(quad)) {
                return quad;
            }
            throw new NoSuchElementException();
        }

        @Override
        public void close() {
            try {
                reader.close();
            } catch (final IOException ex) {
                throw new UncheckedIOException(ex);
            }
        }

        private Boolean isRDFPatchLine(final String line) {
            return line.startsWith("A ") || line.startsWith("D ");
        }

        private Boolean shouldProceed(final String line, final Quad buffer) {
            return nonNull(line) && isNull(buffer);
        }

        private Consumer<Quad> quadHandler(final String prefix) {
            return quad -> {
                if (prefix.equals("D")) {
                    deleted.add(quad);
                } else if (prefix.equals("A") && !deleted.contains(quad)) {
                    buffer = quad;
                }
            };
        }

        private void checkIfMovedIntoTarget(final String line) {
            final Instant moment = parse(line.split(COMMENT_DELIM, 2)[1]);
            if (!time.isBefore(moment)) {
                buffer = rdf.createQuad(Trellis.PreferServerManaged, identifier, DC.modified,
                        rdf.createLiteral(moment.toString(), XSD.dateTime));
                hasModified = true;
            }
        }

        private void tryAdvance() {
            buffer = null;
            while (shouldProceed(line, buffer)) {
                // Determine if the reader is within the target region
                if (inRegion) {
                    // If the reader is in the target region, output any valid "A" quads and record any "D" quads
                    if (isRDFPatchLine(line)) {
                        final String[] parts = line.split(" ", 2);
                        stringToQuad(rdf, parts[1]).ifPresent(quadHandler(parts[0]));

                    // If the reader is in the target region and the modified triple hasn't yet been emitted
                    } else if (line.startsWith(BEGIN) && !hasModified) {
                        checkIfMovedIntoTarget(line);
                    }

                // Check if the reader has entered the target region
                } else if (line.startsWith(END) && !time.isBefore(parse(line.split(COMMENT_DELIM, 2)[1]))) {
                    inRegion = true;
                }

                // Advance the reader
                try {
                    line = reader.readLine();
                } catch (final IOException ex) {
                    throw new UncheckedIOException(ex);
                }
            }
        }
    }

    private RDFPatch() {
        // prevent instantiation
    }
}
