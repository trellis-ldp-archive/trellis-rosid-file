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

import static java.util.Objects.requireNonNull;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toSet;
import static org.trellisldp.spi.RDFUtils.getInstance;

import java.io.File;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;

import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.RDF;
import org.trellisldp.api.Binary;
import org.trellisldp.api.Resource;
import org.trellisldp.rosid.common.ResourceData;
import org.trellisldp.vocabulary.LDP;

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
    public Boolean hasAcl() {
        return ofNullable(data.getHasAcl()).orElse(false);
    }

    @Override
    public IRI getInteractionModel() {
        return ofNullable(data.getLdpType()).map(rdf::createIRI).orElse(LDP.Resource);
    }

    @Override
    public Optional<IRI> getMembershipResource() {
        return ofNullable(data.getMembershipResource()).map(rdf::createIRI);
    }

    @Override
    public Optional<IRI> getMemberRelation() {
        return ofNullable(data.getHasMemberRelation()).map(rdf::createIRI);
    }

    @Override
    public Optional<IRI> getMemberOfRelation() {
        return ofNullable(data.getIsMemberOfRelation()).map(rdf::createIRI);
    }

    @Override
    public Optional<IRI> getInsertedContentRelation() {
        final Optional<IRI> relation = ofNullable(data.getInsertedContentRelation()).map(rdf::createIRI);
        if (!relation.isPresent() && LDP.DirectContainer.equals(getInteractionModel())) {
            return Optional.of(LDP.MemberSubject);
        }
        return relation;
    }

    @Override
    public Optional<IRI> getInbox() {
        return ofNullable(data.getInbox()).map(rdf::createIRI);
    }

    @Override
    public Optional<IRI> getAnnotationService() {
        return ofNullable(data.getAnnotationService()).map(rdf::createIRI);
    }

    @Override
    public Collection<IRI> getTypes() {
        return ofNullable(data.getUserTypes()).orElseGet(Collections::emptyList).stream().map(rdf::createIRI)
            .collect(toSet());
    }

    @Override
    public Optional<Binary> getBinary() {
        return ofNullable(data.getBinary()).map(binary ->
            new Binary(rdf.createIRI(binary.getId()), binary.getModified(), binary.getFormat(), binary.getSize()));
    }

    @Override
    public Instant getModified() {
        return data.getModified();
    }
}
