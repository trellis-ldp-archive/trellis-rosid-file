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

import static java.time.Instant.now;
import static java.time.Instant.parse;
import static java.nio.file.Files.lines;
import static org.apache.commons.rdf.jena.JenaRDF.asTriple;
import static org.apache.jena.riot.Lang.NTRIPLES;
import static org.apache.jena.riot.system.StreamRDFLib.sinkTriples;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;

import org.apache.commons.rdf.api.Graph;
import org.apache.commons.rdf.api.RDF;
import org.apache.commons.rdf.api.Triple;
import org.apache.jena.atlas.lib.SinkToCollection;
import org.apache.jena.riot.RDFParserRegistry;
import org.apache.jena.riot.ReaderRIOT;

/**
 * @author acoburn
 */
class RDFPatchGraphReader {

    private static final ReaderRIOT READER = RDFParserRegistry.getFactory(NTRIPLES).create(NTRIPLES);
    private final RDF rdf;
    private final Instant time;
    private final File file;

    public RDFPatchGraphReader(final RDF rdf, final File file) {
        this(rdf, file, now());
    }

    public RDFPatchGraphReader(final RDF rdf, final File file, final Instant time) {
        this.rdf = rdf;
        this.time = time;
        this.file = file;
    }

    public Graph getGraph() {
        final Graph graph = rdf.createGraph();
        try {
            final Iterator<String> allLines = lines(file.toPath()).iterator();
            while (allLines.hasNext()) {
                final String line = allLines.next();
                if (line.startsWith("D ANY ANY ANY")) {
                    graph.clear();
                } else if (line.startsWith("BEGIN # ")) {
                    if (time.isAfter(parse(line.split(" # ", 2)[1]))) {
                        break;
                    }
                } else if (line.startsWith("A ")) {
                    graph.add(stringToTriple(line.split(" ", 2)[1]));
                } else if (line.startsWith("D ")) {
                    graph.remove(stringToTriple(line.split(" ", 2)[1]));
                }
            }
        } catch (final IOException ex) {
            throw new UncheckedIOException(ex);
        }
        return graph;
    }

    // TODO -- move this to a static utility method
    private Triple stringToTriple(final String line) {
        final List<org.apache.jena.graph.Triple> c = new ArrayList<>();
        READER.read(new StringReader(line), null, NTRIPLES.getContentType(),
                sinkTriples(new SinkToCollection<>(c)), null);
        return asTriple(rdf, c.get(0));
    }
}
