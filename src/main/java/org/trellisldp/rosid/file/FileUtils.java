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

import static java.io.File.separator;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.stream.IntStream.range;
import static org.apache.commons.codec.digest.DigestUtils.md5Hex;
import static org.apache.commons.rdf.jena.JenaRDF.asQuad;
import static org.apache.jena.riot.Lang.NQUADS;
import static org.apache.jena.riot.RDFParser.fromString;
import static org.apache.jena.sparql.core.DatasetGraphFactory.create;

import java.io.File;
import java.net.URI;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.zip.CRC32;

import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.apache.commons.rdf.api.RDF;
import org.apache.jena.sparql.core.DatasetGraph;

/**
 * @author acoburn
 */
public final class FileUtils {

    // The length of the CRC directory partition
    public final static int LENGTH = 2;
    public final static int MAX = 3;

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
        final DatasetGraph dataset = create();
        fromString(line).lang(NQUADS).parse(dataset);
        final Iterator<org.apache.jena.sparql.core.Quad> i = dataset.find();
        if (i.hasNext()) {
            return of(i.next()).map(x -> asQuad(rdf, x));
        }
        return empty();
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
