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

    IRI getIdentifier();

    IRI getOriginalResource();

    Instant getCreated();

    Instant getModified();

    Optional<IRI> getParent();

    Optional<IRI> getAccessControl();

    Optional<IRI> getInbox();

    Stream<Triple> getContainmentTriples();

    Stream<Triple> getMembershipTriples();

    Stream<Triple> getInboundTriples();

    Stream<Triple> getUserTriples();

    Stream<IRI> getTypes();

    Stream<MementoLink> getTimeMap();
}
