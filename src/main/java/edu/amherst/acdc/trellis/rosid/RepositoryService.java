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

import java.time.Duration;
import java.util.Optional;

import org.apache.commons.rdf.api.Graph;
import org.apache.commons.rdf.api.IRI;

import edu.amherst.acdc.trellis.api.Resource;
import edu.amherst.acdc.trellis.spi.EventService;
import edu.amherst.acdc.trellis.spi.ResourceService;
import edu.amherst.acdc.trellis.spi.Session;

/**
 * @author acoburn
 */
public class RepositoryService implements ResourceService {

    private EventService evtSvc;

    @Override
    public void bind(final EventService svc) {
        evtSvc = svc;
    }

    @Override
    public void unbind(final EventService svc) {
        if (evtSvc == svc) {
            evtSvc = null;
        }
    }

    @Override
    public Optional<Resource> find(final Session session, final IRI identifier) {
        // TODO
        return Optional.empty();
    }

    @Override
    public Boolean exists(final Session session, final IRI identifier) {
        // TODO
        return false;
    }

    @Override
    public Resource create(final Session session, final IRI identifier, final IRI type) {
        // TODO
        return null;
    }

    @Override
    public void update(final Session session, final IRI identifier, final Graph graph) {
        // TODO
    }

    @Override
    public void delete(final Session session, final IRI identifier) {
        // TODO
    }

    @Override
    public void commit(final Session session) {
        // TODO
    }

    @Override
    public void expire(final Session session) {
        // TODO
    }

    @Override
    public Session begin() {
        // TODO
        return null;
    }

    @Override
    public Optional<Session> resume(final IRI identifier) {
        // TODO
        return Optional.empty();
    }

    @Override
    public Optional<Session> extend(final Session session, final Duration duration) {
        // TODO
        return Optional.empty();
    }
}
