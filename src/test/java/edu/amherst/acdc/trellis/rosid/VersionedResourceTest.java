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

import static edu.amherst.acdc.trellis.api.Resource.TripleContext.LDP_CONTAINMENT;
import static edu.amherst.acdc.trellis.api.Resource.TripleContext.LDP_MEMBERSHIP;
import static java.time.Instant.parse;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.time.Instant;
import java.util.EnumSet;

import edu.amherst.acdc.trellis.api.Resource;
import edu.amherst.acdc.trellis.vocabulary.LDP;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.RDF;
import org.apache.commons.rdf.jena.JenaRDF;
import org.junit.Test;

/**
 * @author acoburn
 */
public class VersionedResourceTest {

    private static RDF rdf = new JenaRDF();

    @Test
    public void testResource() throws Exception {
        final File file = new File(getClass().getResource("/ldprs").toURI());
        final IRI identifier = rdf.createIRI("info:trellis/ldprs");
        final Instant time = parse("2017-02-15T11:15:00Z");
        final Resource res = VersionedResource.find(file, identifier, time).get();
        assertEquals(identifier, res.getIdentifier());
        assertEquals(LDP.RDFSource, res.getInteractionModel());
        assertEquals(of(rdf.createIRI("info:trellis")), res.getContainedBy());
        assertEquals(empty(), res.getContains().findFirst());
        assertEquals(empty(), res.getMembershipResource());
        assertEquals(empty(), res.getMemberRelation());
        assertEquals(empty(), res.getMemberOfRelation());
        assertEquals(empty(), res.getInsertedContentRelation());
        assertEquals(empty(), res.getDatastream());
        assertTrue(res.isMemento());
        assertFalse(res.isPage());
        assertEquals(empty(), res.getNext());
        assertEquals(of(rdf.createIRI("http://example.org/receiver/inbox")), res.getInbox());
        assertEquals(empty(), res.getAcl());
        assertEquals(parse("2017-02-15T10:05:00Z"), res.getCreated());
        assertEquals(parse("2017-02-15T11:15:00Z"), res.getModified());
        assertEquals(of(rdf.createIRI("http://example.org/user/raadmin")), res.getCreator());
        assertEquals(2L, res.getTypes().count());
        assertTrue(res.getTypes().anyMatch(rdf.createIRI("http://example.org/types/Foo")::equals));
        assertTrue(res.getTypes().anyMatch(rdf.createIRI("http://example.org/types/Bar")::equals));
        assertEquals(0L, res.stream(EnumSet.of(LDP_CONTAINMENT, LDP_MEMBERSHIP)).count());
        // TODO -- test res.getMementos()
        // TODO -- test res.stream(USER_MANAGED)
    }

    @Test
    public void testResourceFuture() throws Exception {
        final File file = new File(getClass().getResource("/ldprs").toURI());
        final IRI identifier = rdf.createIRI("info:trellis/ldprs");
        final Instant time = parse("2017-03-15T11:15:00Z");
        final Resource res = VersionedResource.find(file, identifier, time).get();
        assertEquals(identifier, res.getIdentifier());
        assertEquals(LDP.RDFSource, res.getInteractionModel());
        assertEquals(of(rdf.createIRI("info:trellis")), res.getContainedBy());
        assertEquals(empty(), res.getContains().findFirst());
        assertEquals(empty(), res.getMembershipResource());
        assertEquals(empty(), res.getMemberRelation());
        assertEquals(empty(), res.getMemberOfRelation());
        assertEquals(empty(), res.getInsertedContentRelation());
        assertEquals(empty(), res.getDatastream());
        assertTrue(res.isMemento());
        assertFalse(res.isPage());
        assertEquals(empty(), res.getNext());
        assertEquals(of(rdf.createIRI("http://example.org/receiver/inbox")), res.getInbox());
        assertEquals(empty(), res.getAcl());
        assertEquals(parse("2017-02-15T10:05:00Z"), res.getCreated());
        assertEquals(parse("2017-02-15T11:15:00Z"), res.getModified());
        assertEquals(of(rdf.createIRI("http://example.org/user/raadmin")), res.getCreator());
        assertEquals(2L, res.getTypes().count());
        assertTrue(res.getTypes().anyMatch(rdf.createIRI("http://example.org/types/Foo")::equals));
        assertTrue(res.getTypes().anyMatch(rdf.createIRI("http://example.org/types/Bar")::equals));
        assertEquals(0L, res.stream(EnumSet.of(LDP_CONTAINMENT, LDP_MEMBERSHIP)).count());
        // TODO -- test res.getMementos()
        // TODO -- test res.stream(USER_MANAGED)
    }

    @Test
    public void testResourcePast() throws Exception {
        final File file = new File(getClass().getResource("/ldprs").toURI());
        final IRI identifier = rdf.createIRI("info:trellis/ldprs");
        final Instant time = parse("2017-02-15T11:00:00Z");
        final Resource res = VersionedResource.find(file, identifier, time).get();
        assertEquals(identifier, res.getIdentifier());
        assertEquals(LDP.RDFSource, res.getInteractionModel());
        assertEquals(of(rdf.createIRI("info:trellis")), res.getContainedBy());
        assertEquals(empty(), res.getContains().findFirst());
        assertEquals(empty(), res.getMembershipResource());
        assertEquals(empty(), res.getMemberRelation());
        assertEquals(empty(), res.getMemberOfRelation());
        assertEquals(empty(), res.getInsertedContentRelation());
        assertEquals(empty(), res.getDatastream());
        assertTrue(res.isMemento());
        assertFalse(res.isPage());
        assertEquals(empty(), res.getNext());
        assertEquals(empty(), res.getInbox());
        assertEquals(empty(), res.getAcl());
        assertEquals(parse("2017-02-15T10:05:00Z"), res.getCreated());
        assertEquals(parse("2017-02-15T10:05:00Z"), res.getModified());
        assertEquals(of(rdf.createIRI("http://example.org/user/raadmin")), res.getCreator());
        assertEquals(0L, res.getTypes().count());
        assertEquals(0L, res.stream(EnumSet.of(LDP_CONTAINMENT, LDP_MEMBERSHIP)).count());
        // TODO -- test res.getMementos()
        // TODO -- test res.stream(USER_MANAGED)
    }

    @Test
    public void testResourcePrehistory() throws Exception {
        final File file = new File(getClass().getResource("/ldprs").toURI());
        final IRI identifier = rdf.createIRI("info:trellis/ldprs");
        final Instant time = parse("2017-01-15T11:00:00Z");
        assertFalse(VersionedResource.find(file, identifier, time).isPresent());
    }
}
