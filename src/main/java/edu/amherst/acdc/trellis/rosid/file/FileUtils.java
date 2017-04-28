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
package edu.amherst.acdc.trellis.rosid.file;

import static java.io.File.separator;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.Optional.of;
import static java.util.stream.IntStream.range;
import static org.apache.commons.codec.digest.DigestUtils.md5Hex;
import static org.apache.commons.rdf.jena.JenaRDF.asQuad;
import static org.apache.jena.riot.Lang.NQUADS;
import static org.apache.jena.riot.system.StreamRDFLib.sinkQuads;

import edu.amherst.acdc.trellis.vocabulary.Trellis;

import java.io.File;
import java.io.StringReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.zip.CRC32;

import org.apache.commons.rdf.api.BlankNode;
import org.apache.commons.rdf.api.BlankNodeOrIRI;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.apache.commons.rdf.api.RDF;
import org.apache.commons.rdf.api.RDFTerm;
import org.apache.jena.atlas.lib.SinkToCollection;
import org.apache.jena.riot.RDFParserRegistry;
import org.apache.jena.riot.ReaderRIOT;

/**
 * @author acoburn
 */
final class FileUtils {

    // The length of the CRC directory partition
    public final static int LENGTH = 2;
    public final static int MAX = 3;

    private static final String BNODE_IRI_PREFIX = "trellis:bnode/";

    private static final ReaderRIOT READER = RDFParserRegistry.getFactory(NQUADS).create(NQUADS);

    /**
     * Partition an identifier into a directory structure
     * @param identifier the identifier
     * @return a string usable as a directory path
     */
    public static String partition(final IRI identifier) {
        return partition(identifier.getIRIString());
    }

    /**
     * Partition an identifier into a directory structure
     * @param identifier the identifier
     * @return a string usable as a directory path
     */
    public static String partition(final String identifier) {
        requireNonNull(identifier, "identifier must not be null!");

        final StringJoiner joiner = new StringJoiner(separator);
        final CRC32 hasher = new CRC32();
        hasher.update(identifier.getBytes(UTF_8));
        final String intermediate = Long.toHexString(hasher.getValue());

        range(0, intermediate.length() / LENGTH).limit(MAX)
            .forEach(i -> joiner.add(intermediate.substring(i * LENGTH, (i + 1) * LENGTH)));

        joiner.add(md5Hex(identifier));
        return joiner.toString();
    }

    /**
     * Parse a string into a Quad
     * @param rdf the RDF object
     * @param line the line of text
     * @return the Quad
     */
    public static Optional<Quad> stringToQuad(final RDF rdf, final String line) {
        final List<org.apache.jena.sparql.core.Quad> c = new ArrayList<>();
        READER.read(new StringReader(line), null, NQUADS.getContentType(), sinkQuads(new SinkToCollection<>(c)), null);
        return of(c).filter(x -> !x.isEmpty()).map(x -> asQuad(rdf, x.get(0))).map(quad ->
            rdf.createQuad(quad.getGraphName().orElse(Trellis.PreferUserManaged),
                isSkolemizedBNode(quad.getSubject()) ? deskolemize(rdf, (IRI) quad.getSubject()) : quad.getSubject(),
                quad.getPredicate(),
                isSkolemizedBNode(quad.getObject()) ? deskolemize(rdf, (IRI) quad.getObject()) : quad.getObject()));
    }

    /**
     * Test whether the provide RDF Term is a skolemized blank node
     * @param term the RDF term
     * @return true if this is a skolemized blank node; false otherwise
     */
    public static Boolean isSkolemizedBNode(final RDFTerm term) {
        return term instanceof IRI && term.ntriplesString().startsWith("<" + BNODE_IRI_PREFIX);
    }

    /**
     * Convert an IRI to a BlankNode if it is an internally-skolemized BNode
     * @param rdf the RDF object
     * @param iri the IRI
     * @return the blank node or IRI
     */
    public static BlankNodeOrIRI deskolemize(final RDF rdf, final IRI iri) {
        final String term = iri.getIRIString();
        if (term.startsWith(BNODE_IRI_PREFIX)) {
            return rdf.createBlankNode(term.substring(BNODE_IRI_PREFIX.length(), term.length()));
        }
        return iri;
    }

    /**
     * Skolemize a blank node as an IRI string
     * @param bnode the blank node
     * @return an IRI string
     */
    public static String skolemize(final BlankNode bnode) {
        return "<" + BNODE_IRI_PREFIX + bnode.uniqueReference() + ">";
    }

    /**
     * Get the resource directory for a given identifier
     * @param config the configuration
     * @param identifier the identifier
     * @return the file
     */
    public static File resourceDirectory(final Map<String, String> config, final IRI identifier) {
        return resourceDirectory(config, identifier.getIRIString());
    }

    /**
     * Get the resource directory for a given identifier
     * @param config the configuration
     * @param identifier the identifier
     * @return the file
     */
    public static File resourceDirectory(final Map<String, String> config, final String identifier) {
        final String repo = identifier.split("/")[0].split(":")[1];
        if (config.containsKey(repo)) {
            final File root;
            if (config.get(repo).startsWith("file:")) {
                root = new File(URI.create(config.get(repo)));
            } else {
                root = new File(config.get(repo));
            }
            final File directory = new File(root, partition(identifier));
            directory.mkdirs();
            return directory;
        }
        return null;
    }

    private FileUtils() {
        // prevent instantiation
    }
}
