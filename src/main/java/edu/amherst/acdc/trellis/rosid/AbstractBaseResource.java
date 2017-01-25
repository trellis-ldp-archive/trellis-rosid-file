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

import java.time.Instant;
import java.util.Collection;
import java.util.Optional;
import java.util.stream.Stream;

import edu.amherst.acdc.trellis.api.MementoLink;
import edu.amherst.acdc.trellis.api.Resource;
import edu.amherst.acdc.trellis.vocabulary.LDP;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Triple;

/**
 * @author acoburn
 */
public abstract class AbstractBaseResource implements Resource {

    final protected IRI identifier;
    final protected Instant created;
    final protected Instant modified;

    /**
     * Instantiate a new RdfSource object
     * @param identifier the identifier
     */
    public AbstractBaseResource(final IRI identifier) {
        this.identifier = identifier;
        this.created = Instant.now();
        this.modified = Instant.now();
    }

    @Override
    public IRI getIdentifier() {
        return identifier;
    }

    @Override
    public Optional<IRI> getParent() {
        // TODO
        // parent in memory
        return Optional.empty();
    }

    @Override
    public Stream<MementoLink> getTimeMap() {
        // TODO
        // timemap in memory
        return Stream.empty();
    }

    @Override
    public <T extends Resource.TripleCategory> Stream<Triple> stream(final Collection<T> category) {
        // TODO
        // fetch the triples from storage
        return Stream.empty();
    }

    @Override
    public Optional<IRI> getInbox() {
        // TODO
        // fetch from memory
        return Optional.empty();
    }

    @Override
    public Optional<IRI> getAccessControl() {
        // TODO
        // fetch from memory
        return Optional.empty();
    }

    @Override
    public Stream<IRI> getTypes() {
        // TODO
        // fetch from memory
        return Stream.of(LDP.RDFSource);
    }

    @Override
    public Instant getCreated() {
        return created;
    }

    @Override
    public Instant getModified() {
        return modified;
    }
}
