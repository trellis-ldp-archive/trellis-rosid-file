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

import static java.util.Optional.ofNullable;

import java.io.File;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

//import com.fasterxml.jackson.databind.JsonNode;
import edu.amherst.acdc.trellis.api.Datastream;
import edu.amherst.acdc.trellis.api.MementoLink;
import edu.amherst.acdc.trellis.vocabulary.LDP;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Triple;
import org.apache.commons.rdf.jena.JenaRDF;

/**
 * A resource reader, based on static files.
 *
 * @author acoburn
 */
class FileResourceReader implements ResourceReader {

    private final JenaRDF rdf = new JenaRDF();
    private final IRI identifier;
    private final Map<String, IRI> data;
    private final List<IRI> types;
    private final Instant created;
    private final Instant modified;

    public FileResourceReader(final File base, final IRI identifier) {
        this.identifier = identifier;
        this.data = new HashMap<>();
        this.types = new ArrayList<>();

        // Load the data from a file....
        this.created = Instant.now();
        this.modified = Instant.now();
    }

    @Override
    public IRI getIdentifier() {
        return identifier;
    }

    @Override
    public IRI getOriginal() {
        return data.getOrDefault("identifier", identifier);
    }

    @Override
    public Optional<IRI> getContainedBy() {
        return ofNullable(data.get("containedBy"));
    }

    @Override
    public Optional<IRI> getMembershipResource() {
        return ofNullable(data.get("membershipResource"));
    }

    @Override
    public Optional<IRI> getMemberRelation() {
        return ofNullable(data.get("hasMemberRelation"));
    }

    @Override
    public Optional<IRI> getMemberOfRelation() {
        return ofNullable(data.get("isMemberOfRelation"));
    }

    @Override
    public Optional<IRI> getInsertedContentRelation() {
        return ofNullable(data.get("insertedContentRelation"));
    }

    @Override
    public Optional<IRI> getCreator() {
        return ofNullable(data.get("creator"));
    }

    @Override
    public Optional<IRI> getAcl() {
        return ofNullable(data.get("acl"));
    }

    @Override
    public Optional<IRI> getInbox() {
        return ofNullable(data.get("inbox"));
    }

    @Override
    public Optional<IRI> getTimeMap() {
        // TODO
        return Optional.empty();
    }

    @Override
    public Stream<MementoLink> getMementos() {
        // TODO
        return Stream.empty();
    }

    @Override
    public Boolean isMemento() {
        // TODO
        return false;
    }

    @Override
    public Boolean isPage() {
        // TODO
        return false;
    }

    @Override
    public Optional<IRI> getNext() {
        // TODO
        return Optional.empty();
    }

    @Override
    public Stream<IRI> getTypes() {
        return types.stream();
    }

    @Override
    public Optional<Datastream> getDatastream() {
        // TODO
        return Optional.empty();
    }

    @Override
    public Stream<IRI> getContains() {
        // TODO
        return Stream.empty();
    }

    @Override
    public Instant getCreated() {
        return created;
    }

    @Override
    public Instant getModified() {
        return modified;
    }

    @Override
    public Stream<Triple> getContainmentTriples() {
        return getContains().map(uri -> rdf.createTriple(getIdentifier(), LDP.contains, uri));
    }

    @Override
    public Stream<Triple> getMembershipTriples() {
        // TODO
        return Stream.empty();
    }

    @Override
    public Stream<Triple> getInboundTriples() {
        // TODO
        return Stream.empty();
    }

    @Override
    public Stream<Triple> getUserTriples() {
        // TODO
        return Stream.empty();
    }

}
