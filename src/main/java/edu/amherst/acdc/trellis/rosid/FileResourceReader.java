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

import static java.util.Objects.nonNull;
import static java.util.Optional.ofNullable;
import static edu.amherst.acdc.trellis.api.Resource.TripleContext.FEDORA_INBOUND_REFERENCES;
import static edu.amherst.acdc.trellis.api.Resource.TripleContext.LDP_CONTAINMENT;
import static edu.amherst.acdc.trellis.api.Resource.TripleContext.LDP_MEMBERSHIP;
import static edu.amherst.acdc.trellis.api.Resource.TripleContext.USER_MANAGED;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.amherst.acdc.trellis.api.Resource;
import edu.amherst.acdc.trellis.api.Datastream;
import edu.amherst.acdc.trellis.api.MementoLink;
import edu.amherst.acdc.trellis.vocabulary.LDP;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.RDF;
import org.apache.commons.rdf.api.Triple;
import org.apache.commons.rdf.jena.JenaRDF;

/**
 * A resource reader, based on static files.
 *
 * @author acoburn
 */
public class FileResourceReader implements Resource {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final RDF rdf = new JenaRDF();

    private final IRI identifier;
    private final JsonNode json;
    private final List<IRI> types;
    private final Instant created;
    private final Instant modified;
    private final String version;
    private final String page;

    protected final Map<Resource.TripleContext, Supplier<Stream<Triple>>> mapper = new HashMap<>();

    /**
     * Create a File-based resource reader
     * @param base the data storage directory
     * @param identifier the resource to retrieve
     * @throws IOException if the JSON parsing goes wrong
     */
    public FileResourceReader(final File base, final IRI identifier) throws IOException {
        this(base, identifier, null, null);
    }

    /**
     * Create a File-based resource reader
     * @param base the data storage directory
     * @param identifier the resource to retrieve
     * @param version the version to retreive
     * @param page a session-scoped identifier for the page stream in use
     * @throws IOException if the JSON parsing goes wrong
     */
    public FileResourceReader(final File base, final IRI identifier, final String version, final String page)
            throws IOException {
        this.identifier = identifier;
        this.version = version;
        this.page = page;
        this.types = new ArrayList<>();

        // Load the data from a file....
        // TODO this needs to be an actual file...
        json = MAPPER.readTree(base);
        this.created = Instant.now();
        this.modified = Instant.now();

        // define mappings for triple contexts
        mapper.put(LDP_CONTAINMENT, this::getContainmentTriples);
        mapper.put(LDP_MEMBERSHIP, this::getMembershipTriples);
        mapper.put(FEDORA_INBOUND_REFERENCES, this::getInboundTriples);
        mapper.put(USER_MANAGED, this::getUserTriples);
    }

    @Override
    public IRI getIdentifier() {
        return identifier;
    }

    @Override
    public IRI getInteractionModel() {
        return ofNullable(json.get("interactionModel")).filter(JsonNode::isTextual)
            .map(JsonNode::asText).map(rdf::createIRI).orElse(LDP.Resource);
    }

    @Override
    public IRI getOriginal() {
        return ofNullable(json.get("identifier")).filter(JsonNode::isTextual)
            .map(JsonNode::asText).map(rdf::createIRI).orElse(identifier);
    }

    @Override
    public Optional<IRI> getContainedBy() {
        return ofNullable(json.get("containedBy")).filter(JsonNode::isTextual)
            .map(JsonNode::asText).map(rdf::createIRI);
    }

    @Override
    public Optional<IRI> getMembershipResource() {
        return ofNullable(json.get("membershipResource")).filter(JsonNode::isTextual)
            .map(JsonNode::asText).map(rdf::createIRI);
    }

    @Override
    public Optional<IRI> getMemberRelation() {
        return ofNullable(json.get("hasMemberRelation")).filter(JsonNode::isTextual)
            .map(JsonNode::asText).map(rdf::createIRI);
    }

    @Override
    public Optional<IRI> getMemberOfRelation() {
        return ofNullable(json.get("isMemberOfRelation")).filter(JsonNode::isTextual)
            .map(JsonNode::asText).map(rdf::createIRI);
    }

    @Override
    public Optional<IRI> getInsertedContentRelation() {
        return ofNullable(json.get("insertedContentRelation")).filter(JsonNode::isTextual)
            .map(JsonNode::asText).map(rdf::createIRI);
    }

    @Override
    public Optional<IRI> getCreator() {
        return ofNullable(json.get("creator")).filter(JsonNode::isTextual)
            .map(JsonNode::asText).map(rdf::createIRI);
    }

    @Override
    public Optional<IRI> getAcl() {
        return ofNullable(json.get("acl")).filter(JsonNode::isTextual)
            .map(JsonNode::asText).map(rdf::createIRI);
    }

    @Override
    public Optional<IRI> getInbox() {
        return ofNullable(json.get("inbox")).filter(JsonNode::isTextual)
            .map(JsonNode::asText).map(rdf::createIRI);
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
    public Boolean isMemento() {
        return nonNull(version);
    }

    @Override
    public Boolean isPage() {
        return nonNull(page);
    }

    @Override
    public Optional<IRI> getNext() {
        // TODO -- getIdentifier() + "?page=" + this.page
        // check that there are still triples to consume
        return Optional.empty();
    }

    @Override
    public Stream<IRI> getTypes() {
        // TODO -- this should be read from the json object
        return types.stream();
    }

    @Override
    public Optional<Datastream> getDatastream() {
        // TODO -- this comes from data properties, assembled
        // id = getOriginal() ???
        // format = "format"
        // size = "size"
        // created = "INSTANT"
        // modified = "INSTANT"
        // ^^^ build Datastream object from that
        return Optional.empty();
    }

    @Override
    public Stream<IRI> getContains() {
        // TODO -- read from the data storage
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
    public <T extends Resource.TripleCategory> Stream<Triple> stream(final Collection<T> category) {
        return category.stream().filter(mapper::containsKey).map(mapper::get).map(Supplier::get)
                .reduce(Stream.empty(), Stream::concat);
    }

    private Stream<Triple> getContainmentTriples() {
        return getContains().map(uri -> rdf.createTriple(getIdentifier(), LDP.contains, uri));
    }

    private Stream<Triple> getMembershipTriples() {
        // TODO -- read from data storage
        return Stream.empty();
    }

    private Stream<Triple> getInboundTriples() {
        // TODO -- read from data storage
        return Stream.empty();
    }

    private Stream<Triple> getUserTriples() {
        // TODO -- read from data storage
        return Stream.empty();
    }
}
