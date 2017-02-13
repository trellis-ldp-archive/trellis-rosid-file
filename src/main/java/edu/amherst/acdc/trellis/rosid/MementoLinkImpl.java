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

import edu.amherst.acdc.trellis.api.MementoLink;
import org.apache.commons.rdf.api.IRI;

/**
 * @author acoburn
 */
class MementoLinkImpl implements MementoLink {

    final Instant from;
    final Instant until;
    final IRI identifier;

    public MementoLinkImpl(final IRI identifier, final Instant from, final Instant until) {
        this.identifier = identifier;
        this.from = from;
        this.until = until;
    }

    @Override
    public IRI getIdentifier() {
        return identifier;
    }

    @Override
    public Instant getFrom() {
        return from;
    }

    @Override
    public Instant getUntil() {
        return until;
    }
}
