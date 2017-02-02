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
import static edu.amherst.acdc.trellis.api.Resource.TripleContext.USER_MANAGED;

import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Stream;

import edu.amherst.acdc.trellis.api.MementoLink;
import edu.amherst.acdc.trellis.api.Resource;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Triple;

/**
 * @author acoburn
 */
public abstract class AbstractBaseResource implements Resource {

    final protected ResourceReader resolver;

    final protected Map<Resource.TripleContext, Supplier<Stream<Triple>>> mapper = new HashMap<>();

    /**
     * Instantiate a new RdfSource object
     * @param resolver the resolved resource
     */
    public AbstractBaseResource(final ResourceReader resolver) {
        this.resolver = resolver;
        mapper.put(LDP_CONTAINMENT, resolver::getContainmentTriples);
        mapper.put(LDP_MEMBERSHIP, resolver::getMembershipTriples);
        mapper.put(FEDORA_INBOUND_REFERENCES, resolver::getInboundTriples);
        mapper.put(USER_MANAGED, resolver::getUserTriples);
    }

    @Override
    public IRI getIdentifier() {
        return resolver.getIdentifier();
    }

    @Override
    public IRI getOriginal() {
        return resolver.getOriginal();
    }

    @Override
    public Optional<IRI> getTimeMap() {
        return resolver.getTimeMap();
    }

    @Override
    public Optional<IRI> getContainedBy() {
        return resolver.getContainedBy();
    }

    @Override
    public Stream<MementoLink> getMementos() {
        return resolver.getMementos();
    }

    @Override
    public Boolean isMemento() {
        return resolver.isMemento();
    }

    @Override
    public Boolean isPage() {
        return resolver.isPage();
    }

    @Override
    public Optional<IRI> getNext() {
        return resolver.getNext();
    }

    @Override
    public Optional<IRI> getInbox() {
        return resolver.getInbox();
    }

    @Override
    public Optional<IRI> getAcl() {
        return resolver.getAcl();
    }

    @Override
    public Stream<IRI> getTypes() {
        return resolver.getTypes();
    }

    @Override
    public Optional<IRI> getCreator() {
        return resolver.getCreator();
    }

    @Override
    public Instant getCreated() {
        return resolver.getCreated();
    }

    @Override
    public Instant getModified() {
        return resolver.getModified();
    }

    @Override
    public <T extends Resource.TripleCategory> Stream<Triple> stream(final Collection<T> category) {
        return category.stream().filter(mapper::containsKey).map(mapper::get).map(Supplier::get)
                .reduce(Stream.empty(), Stream::concat);
    }
}
