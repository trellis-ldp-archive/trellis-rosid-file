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
import static edu.amherst.acdc.trellis.api.Resource.TripleContext.TRELLIS_AUDIT;
import static edu.amherst.acdc.trellis.api.Resource.TripleContext.USER_MANAGED;

import java.io.File;
import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Stream;

import edu.amherst.acdc.trellis.api.Resource;
import edu.amherst.acdc.trellis.api.Datastream;
import edu.amherst.acdc.trellis.vocabulary.LDP;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.RDF;
import org.apache.commons.rdf.api.Triple;
import org.apache.commons.rdf.jena.JenaRDF;

/**
 * An object to mediate access to a file-based resource representation
 * @author acoburn
 */
abstract class AbstractFileResource implements Resource {

    protected static final RDF rdf = new JenaRDF();

    protected final IRI identifier;
    protected final File directory;

    protected ResourceData data;

    protected final Map<Resource.TripleContext, Supplier<Stream<Triple>>> mapper = new HashMap<>();

    protected AbstractFileResource(final File directory, final IRI identifier, final ResourceData data) {
        requireNonNull(directory, "The data directory cannot be null!");
        requireNonNull(identifier, "The identifier cannot be null!");
        requireNonNull(data, "The resource data cannot be null!");

        this.identifier = identifier;
        this.directory = directory;
        this.data = data;

        // define mappings for triple contexts
        mapper.put(LDP_CONTAINMENT, this::getContainmentTriples);
        mapper.put(LDP_MEMBERSHIP, this::getMembershipTriples);
        mapper.put(FEDORA_INBOUND_REFERENCES, this::getInboundTriples);
        mapper.put(USER_MANAGED, this::getUserTriples);
        mapper.put(TRELLIS_AUDIT, this::getAuditTriples);
    }

    @Override
    public IRI getIdentifier() {
        return identifier;
    }

    @Override
    public IRI getInteractionModel() {
        return ofNullable(data.ldpType).map(rdf::createIRI).orElse(LDP.Resource);
    }

    @Override
    public Optional<IRI> getContainedBy() {
        return ofNullable(data.containedBy).map(rdf::createIRI);
    }

    @Override
    public Optional<IRI> getMembershipResource() {
        return ofNullable(data.membershipResource).map(rdf::createIRI);
    }

    @Override
    public Optional<IRI> getMemberRelation() {
        return ofNullable(data.hasMemberRelation).map(rdf::createIRI);
    }

    @Override
    public Optional<IRI> getMemberOfRelation() {
        return ofNullable(data.isMemberOfRelation).map(rdf::createIRI);
    }

    @Override
    public Optional<IRI> getInsertedContentRelation() {
        return ofNullable(data.insertedContentRelation).map(rdf::createIRI);
    }

    @Override
    public Optional<IRI> getCreator() {
        return ofNullable(data.creator).map(rdf::createIRI);
    }

    @Override
    public Optional<IRI> getAcl() {
        return ofNullable(data.accessControl).map(rdf::createIRI);
    }

    @Override
    public Optional<IRI> getInbox() {
        return ofNullable(data.inbox).map(rdf::createIRI);
    }

    @Override
    public Stream<IRI> getTypes() {
        return ofNullable(data.userTypes).map(types -> types.stream().map(rdf::createIRI)).orElse(empty());
    }

    @Override
    public Optional<Datastream> getDatastream() {
        return ofNullable(data.datastream).map(ds ->
            new DatastreamImpl(rdf.createIRI(ds.id), ds.created, ds.modified, ds.format, ds.size));
    }

    @Override
    public Instant getCreated() {
        return data.created;
    }

    @Override
    public Instant getModified() {
        return data.modified;
    }

    @Override
    public <T extends Resource.TripleCategory> Stream<Triple> stream(final Collection<T> category) {
        return category.stream().filter(mapper::containsKey).map(mapper::get).flatMap(Supplier::get);
    }

    protected abstract Stream<Triple> getContainmentTriples();

    protected abstract Stream<Triple> getMembershipTriples();

    protected abstract Stream<Triple> getInboundTriples();

    protected abstract Stream<Triple> getUserTriples();

    protected abstract Stream<Triple> getAuditTriples();

}
