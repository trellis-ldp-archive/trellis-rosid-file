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
import static java.nio.file.Files.lines;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.newBufferedWriter;
import static java.nio.file.StandardOpenOption.APPEND;
import static java.time.Instant.now;
import static java.time.Instant.parse;
import static java.util.stream.StreamSupport.stream;
import static org.apache.commons.rdf.jena.JenaRDF.asTriple;
import static org.apache.jena.riot.Lang.NTRIPLES;
import static org.apache.jena.riot.system.StreamRDFLib.sinkTriples;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.apache.commons.io.input.ReversedLinesFileReader;

import org.apache.commons.rdf.api.Graph;
import org.apache.commons.rdf.api.RDF;
import org.apache.commons.rdf.api.Triple;
import org.apache.jena.atlas.lib.SinkToCollection;
import org.apache.jena.riot.RDFParserRegistry;
import org.apache.jena.riot.ReaderRIOT;

/**
 * @author acoburn
 */
class RDFPatch {

    private static final ReaderRIOT READER = RDFParserRegistry.getFactory(NTRIPLES).create(NTRIPLES);

    /**
     * Read the triples from the journal that existed up to (and including) the specified time
     * @param file the file
     * @param time the time
     * @return a stream of RDF Patch statements
     */
    public static Stream<Triple> asStream(final RDF rdf, final File file, final Instant time) {
        return stream(new StreamReader(rdf, file, time), false);
    }

    /**
     * Read the triples from the journal for the current state of the resource
     * @param file the file
     * @return a stream of RDF Patch statements
     */
    public static Stream<Triple> asStream(final RDF rdf, final File file) {
        return asStream(rdf, file, now());
    }

    public static Graph asGraph(final RDF rdf, final File file) {
        return asGraph(rdf, file, now());
    }

    public static Graph asGraph(final RDF rdf, final File file, final Instant time) {
        final Graph graph = rdf.createGraph();
        try {
            final Iterator<String> allLines = lines(file.toPath()).iterator();
            while (allLines.hasNext()) {
                final String line = allLines.next();
                if (line.startsWith("D ANY ANY ANY")) {
                    graph.clear();
                } else if (line.startsWith("BEGIN # ")) {
                    if (time.isBefore(parse(line.split(" # ", 2)[1]))) {
                        break;
                    }
                } else if (line.startsWith("A ")) {
                    graph.add(stringToTriple(rdf, line.split(" ", 2)[1]));
                } else if (line.startsWith("D ")) {
                    graph.remove(stringToTriple(rdf, line.split(" ", 2)[1]));
                }
            }
        } catch (final IOException ex) {
            throw new UncheckedIOException(ex);
        }
        return graph;
    }

    /**
     * Replace RDF Patch statements to the specified file
     * @param file the file
     * @param triples the triples
     * @param time the time
     */
    public static void replace(final File file, final Stream<Triple> triples, final Instant time) {
        try (final BufferedWriter writer = newBufferedWriter(file.toPath(), UTF_8, APPEND)) {
            writer.write("BEGIN # " + time.toString() + lineSeparator());
            writer.write("D ANY ANY ANY ." + lineSeparator());
            triples.forEach(triple -> {
                uncheckedWrite(writer, join(" ", "A", triple.getSubject().ntriplesString(),
                            triple.getPredicate().ntriplesString(), triple.getObject().ntriplesString(), ".") +
                        lineSeparator());
            });
            writer.write("END # " + time.toString() + lineSeparator());
        } catch (final IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    /**
     * Delete RDF Patch statements from the specified file
     * @param file the file
     * @param triples the triples
     * @param time the time
     */
    public static void delete(final File file, final Stream<Triple> triples, final Instant time) {
        try (final BufferedWriter writer = newBufferedWriter(file.toPath(), UTF_8, APPEND)) {
            writer.write("BEGIN # " + time.toString() + lineSeparator());
            triples.forEach(triple -> {
                uncheckedWrite(writer, join(" ", "D", triple.getSubject().ntriplesString(),
                            triple.getPredicate().ntriplesString(), triple.getObject().ntriplesString(), ".") +
                        lineSeparator());
            });
            writer.write("END # " + time.toString() + lineSeparator());
        } catch (final IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    /**
     * Add RDF Patch statements to the specified file
     * @param file the file
     * @param triples the triples
     * @param time the time
     */
    public static void add(final File file, final Stream<Triple> triples, final Instant time) {
        try (final BufferedWriter writer = newBufferedWriter(file.toPath(), UTF_8, APPEND)) {
            writer.write("BEGIN # " + time.toString() + lineSeparator());
            triples.forEach(triple -> {
                uncheckedWrite(writer, join(" ", "A", triple.getSubject().ntriplesString(),
                            triple.getPredicate().ntriplesString(), triple.getObject().ntriplesString(), ".") +
                        lineSeparator());
            });
            writer.write("END # " + time.toString() + lineSeparator());
        } catch (final IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    private static void uncheckedWrite(final Writer writer, final String string) {
        try {
            writer.write(string);
        } catch (final IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    private static Triple stringToTriple(final RDF rdf, final String line) {
        final List<org.apache.jena.graph.Triple> c = new ArrayList<>();
        READER.read(new StringReader(line), null, NTRIPLES.getContentType(),
                sinkTriples(new SinkToCollection<>(c)), null);
        return asTriple(rdf, c.get(0));
    }

    private static class StreamReader implements Spliterator<Triple> {

        private final Set<Triple> deleted = new HashSet<>();
        private final ReversedLinesFileReader reader;
        private final Instant time;
        private final RDF rdf;

        private Boolean inRegion = false;

        /**
         * Create a spliterator that reads a file line-by-line in reverse
         * @param file the file
         */
        public StreamReader(final RDF rdf, final File file) {
            this(rdf, file, now());
        }

        /**
         * Create a spliterator that reads a file line-by-line in reverse
         * @param file the file
         */
        public StreamReader(final RDF rdf, final File file, final Instant time) {
            this.rdf = rdf;
            this.time = time;
            try {
                this.reader = new ReversedLinesFileReader(file, UTF_8);
            } catch (final IOException ex) {
                throw new UncheckedIOException(ex);
            }
        }

        @Override
        public void forEachRemaining(final Consumer<? super Triple> action) {
            try {
                for (String line = reader.readLine(); line != null; line = reader.readLine()) {
                    if (line.startsWith("D ANY ANY ANY") && inRegion) {
                        break;
                    } else if (line.startsWith("END # ")) {
                        final String[] parts = line.split(" # ", 2);
                        if (!time.isBefore(parse(parts[1]))) {
                            inRegion = true;
                        }
                    } else if (inRegion && (line.startsWith("A ") || line.startsWith("D "))) {
                        final String[] parts = line.split(" ", 2);
                        final Triple triple = stringToTriple(rdf, parts[1]);
                        if (parts[0].equals("D")) {
                            deleted.add(triple);
                        } else if (parts[0].equals("A") && !deleted.contains(triple)) {
                            action.accept((Triple) triple);
                        }
                    }
                }
            } catch (final IOException ex) {
                throw new UncheckedIOException(ex);
            }
        }

        @Override
        public boolean tryAdvance(final Consumer<? super Triple> action) {
            try {
                final String line = reader.readLine();
                if (line != null && !(line.startsWith("D ANY ANY ANY") && inRegion)) {
                    if (line.startsWith("END ")) {
                        final String[] parts = line.split(" # ", 2);
                        if (parts.length == 2 && !time.isBefore(parse(parts[1]))) {
                            inRegion = true;
                        }
                    } else if (inRegion && (line.startsWith("A ") || line.startsWith("D "))) {
                        final String[] parts = line.split(" ", 2);
                        final Triple triple = stringToTriple(rdf, parts[1]);
                        if (parts[0].equals("D")) {
                            deleted.add(triple);
                        } else if (parts[0].equals("A") && !deleted.contains(triple)) {
                            action.accept((Triple) triple);
                        }
                    }
                }
                return false;
            } catch (final IOException ex) {
                throw new UncheckedIOException(ex);
            }
        }

        @Override
        public Spliterator<Triple> trySplit() {
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
