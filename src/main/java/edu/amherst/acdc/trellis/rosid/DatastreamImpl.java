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

import java.time.Instant;
import java.util.Optional;

import edu.amherst.acdc.trellis.api.Datastream;
import org.apache.commons.rdf.api.IRI;

/**
 * @author acoburn
 */
class DatastreamImpl implements Datastream {

    private final IRI identifier;
    private final String mimeType;
    private final Long size;
    private final Instant created;
    private final Instant modified;

    /**
     * A simple Datastream object
     * @param identifier the identifier
     * @param created the created date
     * @param modified the modified date
     * @param mimeType the mimeType
     * @param size the size
     */
    public DatastreamImpl(final IRI identifier, final Instant created, final Instant modified,
            final String mimeType, final Long size) {
        requireNonNull(identifier);
        requireNonNull(created);
        requireNonNull(modified);

        this.identifier = identifier;
        this.created = created;
        this.modified = modified;
        this.mimeType = mimeType;
        this.size = size;
    }

    @Override
    public IRI getIdentifier() {
        return identifier;
    }

    @Override
    public Optional<String> getMimeType() {
        return ofNullable(mimeType);
    }

    @Override
    public Optional<Long> getSize() {
        return ofNullable(size);
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
