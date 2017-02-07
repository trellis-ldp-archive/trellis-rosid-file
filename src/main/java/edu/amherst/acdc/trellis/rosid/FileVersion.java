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

import java.io.File;
import java.io.IOException;
import java.util.Optional;
import java.util.stream.Stream;

import edu.amherst.acdc.trellis.api.MementoLink;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Triple;

/**
 * An object that mediates access to the resource version files.
 *
 * @author acoburn
 */
class FileVersion extends AbstractFileResource {

    /**
     * Create a File-based versioned resource reader
     * @param directory the directory
     * @param identifier the resource identifier
     * @throws IOException if the parsing failed
     */
    public FileVersion(final File directory, final IRI identifier) throws IOException {
        super(directory, identifier);

        // TODO -- read from journal to set this.data
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

    protected Stream<Triple> getMembershipTriples() {
        // TODO -- read from data storage
        return Stream.empty();
    }

    protected Stream<Triple> getInboundTriples() {
        // TODO -- read from data storage
        return Stream.empty();
    }

    protected Stream<Triple> getUserTriples() {
        // TODO -- read from data storage
        return Stream.empty();
    }
}
