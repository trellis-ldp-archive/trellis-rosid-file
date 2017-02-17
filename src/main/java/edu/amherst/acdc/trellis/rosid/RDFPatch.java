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
import static java.util.Objects.nonNull;
import static java.util.stream.StreamSupport.stream;
import static org.apache.commons.rdf.jena.JenaRDF.asQuad;
import static org.apache.jena.riot.Lang.NQUADS;
import static org.apache.jena.riot.system.StreamRDFLib.sinkQuads;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Stream;

import edu.amherst.acdc.trellis.api.VersionRange;
import org.apache.commons.io.input.ReversedLinesFileReader;
import org.apache.commons.rdf.api.Graph;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.apache.commons.rdf.api.RDF;
import org.apache.jena.atlas.lib.SinkToCollection;
import org.apache.jena.riot.RDFParserRegistry;
import org.apache.jena.riot.ReaderRIOT;

/**
 * @author acoburn
 */
class RDFPatch {

    private static final ReaderRIOT READER = RDFParserRegistry.getFactory(NQUADS).create(NQUADS);

    /**
     * Read the triples from the journal that existed up to (and including) the specified time
     * @param rdf the rdf object
     * @param file the file
     * @param time the time
     * @return a stream of RDF triples
     */
    public static Stream<Quad> asStream(final RDF rdf, final File file, final Instant time) {
        return stream(new StreamReader(rdf, file, time), false);
    }

    /**
     * Read the triples from the journal for the current state of the resource
     * @param rdf the rdf object
     * @param file the file
     * @return a stream of RDF triples
     */
    public static Stream<Quad> asStream(final RDF rdf, final File file) {
        return asStream(rdf, file, now());
    }

    /**
     * Read the triples from the journal for the current state of the resource
     * @param rdf the rdf object
     * @param file the file
     * @return a graph of the RDF resource
     */
    public static Graph asGraph(final RDF rdf, final File file, final Collection<IRI> category) {
        return asGraph(rdf, file, category, now());
    }

    /**
     * Read the triples from the journal for the resource at a given point in time
     * @param rdf the rdf object
     * @param file the file
     * @param time the time
     * @return a graph of the RDF resource
     */
    public static Graph asGraph(final RDF rdf, final File file, final Collection<IRI> category, final Instant time) {
        final Graph graph = rdf.createGraph();
        try {
            final Iterator<String> allLines = lines(file.toPath()).iterator();
            while (allLines.hasNext()) {
                final String line = allLines.next();
                if (line.startsWith("BEGIN # ")) {
                    if (time.isBefore(parse(line.split(" # ", 2)[1]))) {
                        break;
                    }
                } else if (line.startsWith("A ")) {
                    final Quad quad = stringToQuad(rdf, line.split(" ", 2)[1]);
                    if (quad.getGraphName().filter(category::contains).isPresent()) {
                        graph.add(quad.asTriple());
                    }
                } else if (line.startsWith("D ")) {
                    final Quad quad = stringToQuad(rdf, line.split(" ", 2)[1]);
                    if (quad.getGraphName().filter(category::contains).isPresent()) {
                        graph.remove(quad.asTriple());
                    }
                }
            }
        } catch (final IOException ex) {
            throw new UncheckedIOException(ex);
        }
        return graph;
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
     * @param quads the quads
     * @param time the time
     */
    public static void delete(final File file, final Stream<Quad> quads, final Instant time) {
        try (final BufferedWriter writer = newBufferedWriter(file.toPath(), UTF_8, APPEND)) {
            writer.write("BEGIN # " + time.toString() + lineSeparator());
            quads.filter(quad -> quad.getGraphName().isPresent()).forEach(quad -> {
                uncheckedWrite(writer, join(" ", "D", quad.getSubject().ntriplesString(),
                            quad.getPredicate().ntriplesString(), quad.getObject().ntriplesString(),
                            quad.getGraphName().get().ntriplesString(), ".") + lineSeparator());
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
    public static void add(final File file, final Stream<Quad> quads, final Instant time) {
        try (final BufferedWriter writer = newBufferedWriter(file.toPath(), UTF_8, APPEND)) {
            writer.write("BEGIN # " + time.toString() + lineSeparator());
            quads.filter(quad -> quad.getGraphName().isPresent()).forEach(quad -> {
                uncheckedWrite(writer, join(" ", "A", quad.getSubject().ntriplesString(),
                            quad.getPredicate().ntriplesString(), quad.getObject().ntriplesString(),
                            quad.getGraphName().get().ntriplesString(), ".") + lineSeparator());
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

    private static Quad stringToQuad(final RDF rdf, final String line) {
        final List<org.apache.jena.sparql.core.Quad> c = new ArrayList<>();
        READER.read(new StringReader(line), null, NQUADS.getContentType(),
                sinkQuads(new SinkToCollection<>(c)), null);
        return asQuad(rdf, c.get(0));
    }

    /**
     * A class for reading an RDF Patch file into a VersionRange Spliterator
     */
    private static class TimeMapReader implements Spliterator<VersionRange> {
        private final Iterator<String> dateLines;

        private Instant from = null;

        /**
         * Create a time map reader
         * @param rdf the rdf object
         * @param identifier the identifier
         * @param file the file
         */
        public TimeMapReader(final File file) {
            try {
                dateLines = lines(file.toPath()).filter(line -> line.startsWith("BEGIN # ")).iterator();
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
            final Instant time = parse(line.split(" # ")[1]);
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
        public void forEachRemaining(final Consumer<? super Quad> action) {
            try {
                for (String line = reader.readLine(); nonNull(line); line = reader.readLine()) {
                    if (line.startsWith("END # ")) {
                        final String[] parts = line.split(" # ", 2);
                        if (!time.isBefore(parse(parts[1]))) {
                            inRegion = true;
                        }
                    } else if (inRegion && (line.startsWith("A ") || line.startsWith("D "))) {
                        final String[] parts = line.split(" ", 2);
                        final Quad quad = stringToQuad(rdf, parts[1]);
                        if (parts[0].equals("D")) {
                            deleted.add(quad);
                        } else if (parts[0].equals("A") && !deleted.contains(quad)) {
                            action.accept((Quad) quad);
                        }
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
                    if (line.startsWith("END ")) {
                        final String[] parts = line.split(" # ", 2);
                        if (parts.length == 2 && !time.isBefore(parse(parts[1]))) {
                            inRegion = true;
                        }
                    } else if (inRegion && (line.startsWith("A ") || line.startsWith("D "))) {
                        final String[] parts = line.split(" ", 2);
                        final Quad quad = stringToQuad(rdf, parts[1]);
                        if (parts[0].equals("D")) {
                            deleted.add(quad);
                        } else if (parts[0].equals("A") && !deleted.contains(quad)) {
                            action.accept((Quad) quad);
                        }
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
