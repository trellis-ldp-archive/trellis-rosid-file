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

import static edu.amherst.acdc.trellis.rosid.Constants.RESOURCE_JOURNAL;

import java.io.File;
import java.time.Instant;
import java.util.Optional;
import java.util.stream.Stream;

import edu.amherst.acdc.trellis.api.MementoLink;
import edu.amherst.acdc.trellis.vocabulary.XSD;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Literal;
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
        this.data = read(directory);
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
        final Literal literal = rdf.createLiteral(time.toString(), XSD.dateTime);
        RDFPatch.write(journal, statements, identifier, literal, agent);
    }

    private static ResourceData read(final File directory, final Instant time) {
        // TODO -- populate rd with triple data
        final ResourceData rd = new ResourceData();
        final Stream<Triple> triples = RDFPatch.read(new File(directory, RESOURCE_JOURNAL));
        return rd;
    }

    private static ResourceData read(final File directory) {
        // TODO -- populate rd with triple data
        final ResourceData rd = new ResourceData();
        final Stream<Triple> triples = RDFPatch.read(new File(directory, RESOURCE_JOURNAL));
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
}
