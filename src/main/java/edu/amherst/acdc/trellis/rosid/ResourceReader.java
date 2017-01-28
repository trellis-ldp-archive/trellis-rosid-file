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
import java.util.Optional;
import java.util.stream.Stream;

import edu.amherst.acdc.trellis.api.MementoLink;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Triple;

/**
 * An interface for reading a resource from some data store.
 *
 * @author acoburn
 */
public interface ResourceReader {

    /**
     * Get the resource identifier
     * @return the identifier
     */
    IRI getIdentifier();

    /**
     * Get the identifier for the original resource
     * @return the original resource identifier
     */
    IRI getOriginal();

    /**
     * Get the created date for the resource
     * @return the created date
     */
    Instant getCreated();

    /**
     * Get the modified date for the resource
     * @return the modified date
     */
    Instant getModified();

    /**
     * Get the identifier of the resource that contains this resource
     * @return the containing resource identifier
     */
    Optional<IRI> getContainedBy();

    /**
     * Get the ACL resource identifier
     * @return the identifier for the acl
     */
    Optional<IRI> getAcl();

    /**
     * Get the inbox resource identifier
     * @return the inbox resource identifier
     */
    Optional<IRI> getInbox();

    /**
     * Get the containment triples
     * @return a stream of containment triples
     */
    Stream<Triple> getContainmentTriples();

    /**
     * Get the membership triples
     * @return a stream of membership triples
     */
    Stream<Triple> getMembershipTriples();

    /**
     * Get the inbound reference triples
     * @return a stream of inbound references
     */
    Stream<Triple> getInboundTriples();

    /**
     * Get the user-created triples
     * @return a stream of user-created triples
     */
    Stream<Triple> getUserTriples();

    /**
     * Get the resource types
     * @return a stream of resource types
     */
    Stream<IRI> getTypes();

    /**
     * Get the Memento link resources
     * @return a stream of Memento resources
     */
    Stream<MementoLink> getMementos();
}
