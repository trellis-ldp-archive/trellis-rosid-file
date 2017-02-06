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

import static java.util.Objects.requireNonNull;
import static java.util.Optional.ofNullable;
import static java.util.stream.Stream.empty;
import static edu.amherst.acdc.trellis.api.Resource.TripleContext.FEDORA_INBOUND_REFERENCES;
import static edu.amherst.acdc.trellis.api.Resource.TripleContext.LDP_CONTAINMENT;
import static edu.amherst.acdc.trellis.api.Resource.TripleContext.LDP_MEMBERSHIP;
import static edu.amherst.acdc.trellis.api.Resource.TripleContext.USER_MANAGED;
import static edu.amherst.acdc.trellis.rosid.Constants.RESOURCE_CACHE;
import static com.fasterxml.jackson.databind.SerializationFeature.WRITE_DATES_AS_TIMESTAMPS;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
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
class FileCache implements Resource {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    static {
        MAPPER.configure(WRITE_DATES_AS_TIMESTAMPS, false);
        MAPPER.registerModule(new JavaTimeModule());
    }

    private static final RDF rdf = new JenaRDF();

    private final IRI identifier;
    private final JsonResource json;

    protected final Map<Resource.TripleContext, Supplier<Stream<Triple>>> mapper = new HashMap<>();

    /**
     * Create a File-based resource reader
     * @param directory the data storage directory
     * @param identifier the resource to retrieve
     * @throws IOException if the JSON parsing goes wrong
     */
    public FileCache(final File directory, final IRI identifier) throws IOException {
        requireNonNull(directory, "The data directory cannot be null!");
        requireNonNull(identifier, "The identifier cannot be null!");

        this.identifier = identifier;

        json = MAPPER.readValue(new File(directory, RESOURCE_CACHE), JsonResource.class);

        // define mappings for triple contexts
        mapper.put(LDP_CONTAINMENT, this::getContainmentTriples);
        mapper.put(LDP_MEMBERSHIP, this::getMembershipTriples);
        mapper.put(FEDORA_INBOUND_REFERENCES, this::getInboundTriples);
        mapper.put(USER_MANAGED, this::getUserTriples);
    }

    public static void write(final File directory, final JsonResource json) throws IOException {
        MAPPER.writeValue(new File(directory, RESOURCE_CACHE), json);
    }

    @Override
    public IRI getIdentifier() {
        return identifier;
    }

    @Override
    public IRI getInteractionModel() {
        return ofNullable(json.ldpType).map(rdf::createIRI).orElse(LDP.Resource);
    }

    @Override
    public IRI getOriginal() {
        return ofNullable(json.id).map(rdf::createIRI).orElse(identifier);
    }

    @Override
    public Optional<IRI> getContainedBy() {
        return ofNullable(json.containedBy).map(rdf::createIRI);
    }

    @Override
    public Optional<IRI> getMembershipResource() {
        return ofNullable(json.membershipResource).map(rdf::createIRI);
    }

    @Override
    public Optional<IRI> getMemberRelation() {
        return ofNullable(json.hasMemberRelation).map(rdf::createIRI);
    }

    @Override
    public Optional<IRI> getMemberOfRelation() {
        return ofNullable(json.isMemberOfRelation).map(rdf::createIRI);
    }

    @Override
    public Optional<IRI> getInsertedContentRelation() {
        return ofNullable(json.insertedContentRelation).map(rdf::createIRI);
    }

    @Override
    public Optional<IRI> getCreator() {
        return ofNullable(json.creator).map(rdf::createIRI);
    }

    @Override
    public Optional<IRI> getAcl() {
        return ofNullable(json.accessControl).map(rdf::createIRI);
    }

    @Override
    public Optional<IRI> getInbox() {
        return ofNullable(json.inbox).map(rdf::createIRI);
    }

    @Override
    public Stream<IRI> getTypes() {
        return ofNullable(json.userTypes).map(types -> types.stream().map(rdf::createIRI)).orElse(empty());
    }

    @Override
    public Optional<Datastream> getDatastream() {
        return ofNullable(json.datastream).map(ds ->
            new DatastreamImpl(rdf.createIRI(ds.id),
                    ds.created, ds.modified, ds.format, ds.size));
    }

    @Override
    public Instant getCreated() {
        return json.created;
    }

    @Override
    public Instant getModified() {
        return json.modified;
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
