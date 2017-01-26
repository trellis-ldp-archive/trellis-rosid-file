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

import static java.util.Optional.ofNullable;

import java.io.File;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import edu.amherst.acdc.trellis.api.MementoLink;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Triple;

/**
 * A resource reader, based on static files.
 *
 * @author acoburn
 */
class FileResourceReader implements ResourceReader {

    private final IRI identifier;
    private final Map<String, IRI> data;
    private final List<IRI> types;
    private final Instant created;
    private final Instant modified;

    public FileResourceReader(final File base, final IRI identifier) {
        this.identifier = identifier;
        this.data = new HashMap<>();
        this.types = new ArrayList<>();

        // Load the data from a file....
        this.created = Instant.now();
        this.modified = Instant.now();
    }

    @Override
    public IRI getIdentifier() {
        return identifier;
    }

    @Override
    public IRI getOriginalResource() {
        return data.getOrDefault("canonical", identifier);
    }

    @Override
    public Instant getCreated() {
        return created;
    }

    @Override
    public Instant getModified() {
        return modified;
    }

    @Override
    public Optional<IRI> getParent() {
        return ofNullable(data.get("parent"));
    }

    @Override
    public Optional<IRI> getAccessControl() {
        return ofNullable(data.get("acl"));
    }

    @Override
    public Optional<IRI> getInbox() {
        return ofNullable(data.get("inbox"));
    }

    @Override
    public Stream<IRI> getTypes() {
        return types.stream();
    }

    @Override
    public Stream<Triple> getContainmentTriples() {
        // TODO
        return Stream.empty();
    }

    @Override
    public Stream<Triple> getMembershipTriples() {
        // TODO
        return Stream.empty();
    }

    @Override
    public Stream<Triple> getInboundTriples() {
        // TODO
        return Stream.empty();
    }

    @Override
    public Stream<Triple> getUserTriples() {
        // TODO
        return Stream.empty();
    }

    @Override
    public Stream<MementoLink> getTimeMap() {
        // TODO
        return Stream.empty();
    }
}
