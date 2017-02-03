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

import static java.time.Instant.parse;
import static java.util.Objects.requireNonNull;
import static java.util.Optional.ofNullable;
import static java.util.stream.Stream.builder;
import static java.util.stream.Stream.empty;
import static edu.amherst.acdc.trellis.api.Resource.TripleContext.FEDORA_INBOUND_REFERENCES;
import static edu.amherst.acdc.trellis.api.Resource.TripleContext.LDP_CONTAINMENT;
import static edu.amherst.acdc.trellis.api.Resource.TripleContext.LDP_MEMBERSHIP;
import static edu.amherst.acdc.trellis.api.Resource.TripleContext.USER_MANAGED;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
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
public class FileCacheReader implements Resource {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final RDF rdf = new JenaRDF();

    private final IRI identifier;
    private final JsonNode json;

    protected final Map<Resource.TripleContext, Supplier<Stream<Triple>>> mapper = new HashMap<>();

    /**
     * Create a File-based resource reader
     * @param base the data storage directory
     * @param identifier the resource to retrieve
     * @throws IOException if the JSON parsing goes wrong
     */
    public FileCacheReader(final File base, final IRI identifier) throws IOException {
        requireNonNull(base, "The data directory cannot be null!");
        requireNonNull(identifier, "The identifier cannot be null!");

        this.identifier = identifier;

        // Load the data from a file....
        // TODO this needs to be an actual file based on the identifier
        json = MAPPER.readTree(base);

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
    public Stream<IRI> getTypes() {
        return ofNullable(json.get("@type")).map(this::jsonNodeToStream).orElse(empty()).map(rdf::createIRI);
    }

    @Override
    public Optional<Datastream> getDatastream() {
        return ofNullable(json.get("datastream")).map(node ->
            new DatastreamImpl(rdf.createIRI(node.get("@id").asText()),
                parse(node.get("created").asText()),
                parse(node.get("modified").asText()),
                ofNullable(node.get("format")).map(JsonNode::asText).orElse(null),
                ofNullable(node.get("size")).map(JsonNode::asLong).orElse(null)));
    }

    @Override
    public Instant getCreated() {
        return parse(json.get("created").asText());
    }

    @Override
    public Instant getModified() {
        return parse(json.get("modified").asText());
    }

    @Override
    public <T extends Resource.TripleCategory> Stream<Triple> stream(final Collection<T> category) {
        return category.stream().filter(mapper::containsKey).map(mapper::get).map(Supplier::get)
                .reduce(Stream.empty(), Stream::concat);
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

    private Stream<String> jsonNodeToStream(final JsonNode node) {
        final Stream.Builder<JsonNode> items = builder();
        if (node.isArray()) {
            for (final JsonNode item : node) {
                items.accept(item);
            }
        } else {
            items.accept(node);
        }
        return items.build().filter(JsonNode::isTextual).map(JsonNode::asText);
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
