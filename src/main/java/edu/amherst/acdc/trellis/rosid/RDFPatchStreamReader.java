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

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.Instant.now;
import static java.time.Instant.parse;
import static java.util.Spliterator.ORDERED;
import static java.util.Spliterator.IMMUTABLE;
import static java.util.Spliterator.NONNULL;
import static org.apache.commons.rdf.jena.JenaRDF.asTriple;
import static org.apache.jena.riot.Lang.NTRIPLES;
import static org.apache.jena.riot.system.StreamRDFLib.sinkTriples;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Spliterator;
import java.util.function.Consumer;

import org.apache.commons.io.input.ReversedLinesFileReader;
import org.apache.commons.rdf.api.RDF;
import org.apache.commons.rdf.api.Triple;
import org.apache.jena.atlas.lib.SinkToCollection;
import org.apache.jena.riot.RDFParserRegistry;
import org.apache.jena.riot.ReaderRIOT;

/**
 * A spliterator class that allows reading files line-by-line in reverse
 *
 * @author acoburn
 */
class RDFPatchStreamReader implements Spliterator<Triple> {

    protected static final ReaderRIOT READER = RDFParserRegistry.getFactory(NTRIPLES).create(NTRIPLES);

    private final Set<Triple> deleted = new HashSet<>();
    private final ReversedLinesFileReader reader;
    private final Instant time;
    private final RDF rdf;

    private Boolean inRegion = false;

    /**
     * Create a spliterator that reads a file line-by-line in reverse
     * @param file the file
     */
    public RDFPatchStreamReader(final RDF rdf, final File file) {
        this(rdf, file, now());
    }

    /**
     * Create a spliterator that reads a file line-by-line in reverse
     * @param file the file
     */
    public RDFPatchStreamReader(final RDF rdf, final File file, final Instant time) {
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
                    final Triple triple = stringToTriple(parts[1]);
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
                    final Triple triple = stringToTriple(parts[1]);
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

    // TODO move this to a static utility method (see RDFPatchGraphReader)
    private Triple stringToTriple(final String line) {
        final List<org.apache.jena.graph.Triple> c = new ArrayList<>();
        READER.read(new StringReader(line), null, NTRIPLES.getContentType(),
                sinkTriples(new SinkToCollection<>(c)), null);
        return asTriple(rdf, c.get(0));
    }
}
