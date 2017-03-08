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

import static edu.amherst.acdc.trellis.api.Resource.TripleContext.FEDORA_INBOUND_REFERENCES;
import static edu.amherst.acdc.trellis.api.Resource.TripleContext.LDP_CONTAINMENT;
import static edu.amherst.acdc.trellis.api.Resource.TripleContext.LDP_MEMBERSHIP;
import static edu.amherst.acdc.trellis.api.Resource.TripleContext.TRELLIS_AUDIT;
import static edu.amherst.acdc.trellis.api.Resource.TripleContext.USER_MANAGED;
import static java.util.Collections.singleton;
import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;
import static java.util.Optional.ofNullable;
import static java.util.stream.Stream.empty;
import static java.util.stream.Stream.of;

import edu.amherst.acdc.trellis.api.Datastream;
import edu.amherst.acdc.trellis.api.Resource;
import edu.amherst.acdc.trellis.vocabulary.Fedora;
import edu.amherst.acdc.trellis.vocabulary.LDP;
import edu.amherst.acdc.trellis.vocabulary.Trellis;

import java.io.File;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

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

    protected static final Map<IRI, Resource.TripleCategory> categorymap = unmodifiableMap(
        new HashMap<IRI, Resource.TripleCategory>() { {
            put(Fedora.PreferInboundReferences, FEDORA_INBOUND_REFERENCES);
            put(LDP.PreferContainment, LDP_CONTAINMENT);
            put(LDP.PreferMembership, LDP_MEMBERSHIP);
            put(Trellis.PreferAudit, TRELLIS_AUDIT);
            put(Trellis.PreferUserManaged, USER_MANAGED);
    }});

    protected final IRI identifier;
    protected final File directory;
    protected final ResourceData data;

    /**
     * An abstract contructor for creating a file-based resource
     * @param directory the directory
     * @param identifier the identifier
     * @param data the data
     */
    protected AbstractFileResource(final File directory, final IRI identifier, final ResourceData data) {
        requireNonNull(directory, "The data directory cannot be null!");
        requireNonNull(identifier, "The identifier cannot be null!");
        requireNonNull(data, "The resource data cannot be null!");

        this.identifier = identifier;
        this.directory = directory;
        this.data = data;
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
    public Optional<IRI> getAnnotationService() {
        return ofNullable(data.annotationService).map(rdf::createIRI);
    }

    @Override
    public Stream<IRI> getTypes() {
        return ofNullable(data.userTypes).map(types -> types.stream().map(rdf::createIRI)).orElse(empty());
    }

    @Override
    public Optional<Datastream> getDatastream() {
        return ofNullable(data.datastream).map(ds ->
            new Datastream(rdf.createIRI(ds.id), ds.created, ds.modified, ds.format, ds.size));
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
    public Stream<IRI> getContains() {
        return stream(singleton(LDP_CONTAINMENT)).map(Triple::getObject).flatMap(obj -> {
            if (obj instanceof IRI) {
                return of((IRI) obj);
            }
            return empty();
        });
    }
}
