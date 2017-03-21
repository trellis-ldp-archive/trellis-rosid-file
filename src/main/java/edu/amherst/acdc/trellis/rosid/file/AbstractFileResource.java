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

import static edu.amherst.acdc.trellis.rosid.common.RDFUtils.getInstance;
import static java.util.Objects.requireNonNull;
import static java.util.Optional.ofNullable;
import static java.util.stream.Stream.empty;
import static java.util.stream.Stream.of;

import edu.amherst.acdc.trellis.api.Datastream;
import edu.amherst.acdc.trellis.api.Resource;
import edu.amherst.acdc.trellis.rosid.common.ResourceData;
import edu.amherst.acdc.trellis.vocabulary.LDP;

import java.io.File;
import java.time.Instant;
import java.util.Optional;
import java.util.stream.Stream;

import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.apache.commons.rdf.api.RDF;

/**
 * An object to mediate access to a file-based resource representation
 * @author acoburn
 */
abstract class AbstractFileResource implements Resource {

    protected static final RDF rdf = getInstance();

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
        return getParent(getIdentifier().getIRIString().split("#")[0].split("\\?")[0]).map(rdf::createIRI);
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
        return stream().filter(quad -> quad.getGraphName().filter(LDP.PreferContainment::equals).isPresent())
            .map(Quad::getObject).flatMap(obj -> {
                if (obj instanceof IRI) {
                    return of((IRI) obj);
                }
                return empty();
            });
    }

    private static Optional<String> getParent(final String iri) {
        if (iri.endsWith("/")) {
            return getParent(iri.substring(0, iri.length() - 1));
        }
        return Optional.of(iri.lastIndexOf("/")).filter(idx -> idx > 0).map(idx -> iri.substring(0, idx));
    }
}
