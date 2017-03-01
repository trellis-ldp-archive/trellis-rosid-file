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
import static edu.amherst.acdc.trellis.vocabulary.RDF.type;
import static java.time.Instant.parse;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.time.Instant;
import java.util.EnumSet;
import java.util.List;

import edu.amherst.acdc.trellis.api.Resource;
import edu.amherst.acdc.trellis.api.VersionRange;
import edu.amherst.acdc.trellis.vocabulary.DC;
import edu.amherst.acdc.trellis.vocabulary.LDP;
import edu.amherst.acdc.trellis.vocabulary.RDFS;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.RDF;
import org.apache.commons.rdf.api.Triple;
import org.apache.commons.rdf.jena.JenaRDF;
import org.junit.Before;
import org.junit.Test;

/**
 * @author acoburn
 */
public class LdpContainerTest {

    private static final RDF rdf = new JenaRDF();

    private File file;
    private IRI identifier = rdf.createIRI("info:trellis/ldpc");

    @Before
    public void setUp() throws Exception {
        file = new File(getClass().getResource("/ldpc").toURI());
    }

    @Test
    public void testVersionedResource() {
        final Instant time = parse("2017-02-16T11:15:03Z");
        final Resource res = VersionedResource.find(file, identifier, time).get();
        assertEquals(identifier, res.getIdentifier());
        assertEquals(LDP.RDFSource, res.getInteractionModel());
        assertEquals(of(rdf.createIRI("info:trellis")), res.getContainedBy());
        final List<IRI> contained = res.getContains().collect(toList());
        assertEquals(3L, contained.size());
        assertTrue(contained.contains(rdf.createIRI("info:trellis/ldpc/1")));
        assertTrue(contained.contains(rdf.createIRI("info:trellis/ldpc/2")));
        assertTrue(contained.contains(rdf.createIRI("info:trellis/ldpc/3")));
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
        assertEquals(parse("2017-02-16T11:15:03Z"), res.getModified());
        assertEquals(of(rdf.createIRI("http://example.org/user/raadmin")), res.getCreator());
        assertEquals(2L, res.getTypes().count());
        assertTrue(res.getTypes().anyMatch(rdf.createIRI("http://example.org/types/Foo")::equals));
        assertTrue(res.getTypes().anyMatch(rdf.createIRI("http://example.org/types/Bar")::equals));
        assertEquals(3L, res.stream(EnumSet.of(LDP_CONTAINMENT)).count());
        assertEquals(0L, res.stream(EnumSet.of(LDP_MEMBERSHIP)).count());

        final List<VersionRange> mementos = res.getMementos().collect(toList());
        assertEquals(1L, mementos.size());
        assertEquals(parse("2017-02-15T10:05:00Z"), mementos.get(0).getFrom());
        assertEquals(parse("2017-02-15T11:15:00Z"), mementos.get(0).getUntil());

        final List<Triple> triples = res.stream(USER_MANAGED).collect(toList());
        assertEquals(5L, triples.size());
        assertTrue(triples.contains(rdf.createTriple(identifier, LDP.inbox,
                        rdf.createIRI("http://example.org/receiver/inbox"))));
        assertTrue(triples.contains(rdf.createTriple(identifier, type,
                        rdf.createIRI("http://example.org/types/Foo"))));
        assertTrue(triples.contains(rdf.createTriple(identifier, type,
                        rdf.createIRI("http://example.org/types/Bar"))));
        assertTrue(triples.contains(rdf.createTriple(identifier, RDFS.label,
                        rdf.createLiteral("A label", "eng"))));
        assertTrue(triples.contains(rdf.createTriple(rdf.createIRI("http://example.org/some/other/resource"),
                    RDFS.label, rdf.createLiteral("Some other resource", "eng"))));

        final List<Triple> inbound = res.stream(FEDORA_INBOUND_REFERENCES).collect(toList());
        assertEquals(2L, inbound.size());
        assertTrue(inbound.contains(rdf.createTriple(rdf.createIRI("info:trellis/resource"),
                        DC.hasPart, identifier)));
        assertTrue(inbound.contains(rdf.createTriple(rdf.createIRI("info:trellis/other/resource"),
                        DC.relation, identifier)));
    }

    @Test
    public void testResourceFuture() {
        final Instant time = parse("2017-03-15T11:15:00Z");
        final Resource res = VersionedResource.find(file, identifier, time).get();
        assertEquals(identifier, res.getIdentifier());
        assertEquals(LDP.RDFSource, res.getInteractionModel());
        assertEquals(of(rdf.createIRI("info:trellis")), res.getContainedBy());
        final List<IRI> contained = res.getContains().collect(toList());
        assertEquals(3L, contained.size());
        assertTrue(contained.contains(rdf.createIRI("info:trellis/ldpc/1")));
        assertTrue(contained.contains(rdf.createIRI("info:trellis/ldpc/2")));
        assertTrue(contained.contains(rdf.createIRI("info:trellis/ldpc/3")));
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
        assertEquals(parse("2017-02-16T11:15:03Z"), res.getModified());
        assertEquals(of(rdf.createIRI("http://example.org/user/raadmin")), res.getCreator());
        assertEquals(2L, res.getTypes().count());
        assertTrue(res.getTypes().anyMatch(rdf.createIRI("http://example.org/types/Foo")::equals));
        assertTrue(res.getTypes().anyMatch(rdf.createIRI("http://example.org/types/Bar")::equals));
        assertEquals(3L, res.stream(EnumSet.of(LDP_CONTAINMENT)).count());
        assertEquals(0L, res.stream(LDP_MEMBERSHIP).count());

        final List<Triple> triples = res.stream(USER_MANAGED).collect(toList());
        assertEquals(5L, triples.size());
        assertTrue(triples.contains(rdf.createTriple(identifier, LDP.inbox,
                        rdf.createIRI("http://example.org/receiver/inbox"))));
        assertTrue(triples.contains(rdf.createTriple(identifier, type,
                        rdf.createIRI("http://example.org/types/Foo"))));
        assertTrue(triples.contains(rdf.createTriple(identifier, type,
                        rdf.createIRI("http://example.org/types/Bar"))));
        assertTrue(triples.contains(rdf.createTriple(identifier, RDFS.label,
                        rdf.createLiteral("A label", "eng"))));
        assertTrue(triples.contains(rdf.createTriple(rdf.createIRI("http://example.org/some/other/resource"),
                    RDFS.label, rdf.createLiteral("Some other resource", "eng"))));

        final List<Triple> inbound = res.stream(FEDORA_INBOUND_REFERENCES).collect(toList());
        assertEquals(3L, inbound.size());
        assertTrue(inbound.contains(rdf.createTriple(rdf.createIRI("info:trellis/resource"),
                        DC.hasPart, identifier)));
        assertTrue(inbound.contains(rdf.createTriple(rdf.createIRI("info:trellis/other/resource"),
                        DC.relation, identifier)));
        assertTrue(inbound.contains(rdf.createTriple(rdf.createIRI("info:trellis/other/item"),
                        DC.hasPart, identifier)));

        final List<VersionRange> mementos = res.getMementos().collect(toList());
        assertEquals(1L, mementos.size());
        assertEquals(parse("2017-02-15T10:05:00Z"), mementos.get(0).getFrom());
        assertEquals(parse("2017-02-15T11:15:00Z"), mementos.get(0).getUntil());
    }

    @Test
    public void testResourcePast() {
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

        final List<Triple> triples = res.stream(USER_MANAGED).collect(toList());
        assertEquals(0L, triples.size());

        final List<Triple> inbound = res.stream(FEDORA_INBOUND_REFERENCES).collect(toList());
        assertEquals(2L, inbound.size());
        assertTrue(inbound.contains(rdf.createTriple(rdf.createIRI("info:trellis/resource"),
                        DC.hasPart, identifier)));
        assertTrue(inbound.contains(rdf.createTriple(rdf.createIRI("info:trellis/other/resource"),
                        DC.relation, identifier)));

        final List<VersionRange> mementos = res.getMementos().collect(toList());
        assertEquals(1L, mementos.size());
        assertEquals(parse("2017-02-15T10:05:00Z"), mementos.get(0).getFrom());
        assertEquals(parse("2017-02-15T11:15:00Z"), mementos.get(0).getUntil());
    }

    @Test
    public void testResourcePrehistory() {
        final Instant time = parse("2017-01-15T11:00:00Z");
        assertFalse(VersionedResource.find(file, identifier, time).isPresent());
    }

    @Test
    public void testCachedResource() {
        final Resource res = CachedResource.find(file, identifier).get();
        assertEquals(identifier, res.getIdentifier());
        assertEquals(LDP.RDFSource, res.getInteractionModel());
        assertEquals(of(rdf.createIRI("info:trellis")), res.getContainedBy());
        final List<IRI> contained = res.getContains().collect(toList());
        assertEquals(3L, contained.size());
        System.out.println(contained.toString());
        assertTrue(contained.contains(rdf.createIRI("info:trellis/ldpc/1")));
        assertTrue(contained.contains(rdf.createIRI("info:trellis/ldpc/2")));
        assertTrue(contained.contains(rdf.createIRI("info:trellis/ldpc/3")));
        assertEquals(empty(), res.getMembershipResource());
        assertEquals(empty(), res.getMemberRelation());
        assertEquals(empty(), res.getMemberOfRelation());
        assertEquals(empty(), res.getInsertedContentRelation());
        assertEquals(empty(), res.getDatastream());
        assertFalse(res.isMemento());
        assertFalse(res.isPage());
        assertEquals(empty(), res.getNext());
        assertEquals(of(rdf.createIRI("http://example.org/receiver/inbox")), res.getInbox());
        assertEquals(empty(), res.getAcl());
        assertEquals(parse("2017-02-15T10:05:00Z"), res.getCreated());
        assertEquals(parse("2017-02-16T11:15:03Z"), res.getModified());
        assertEquals(of(rdf.createIRI("http://example.org/user/raadmin")), res.getCreator());
        assertEquals(2L, res.getTypes().count());
        assertTrue(res.getTypes().anyMatch(rdf.createIRI("http://example.org/types/Foo")::equals));
        assertTrue(res.getTypes().anyMatch(rdf.createIRI("http://example.org/types/Bar")::equals));
        assertEquals(3L, res.stream(LDP_CONTAINMENT).count());
        assertEquals(0L, res.stream(EnumSet.of(LDP_MEMBERSHIP)).count());

        final List<Triple> triples = res.stream(USER_MANAGED).collect(toList());
        assertEquals(5L, triples.size());
        assertTrue(triples.contains(rdf.createTriple(identifier, LDP.inbox,
                        rdf.createIRI("http://example.org/receiver/inbox"))));
        assertTrue(triples.contains(rdf.createTriple(identifier, type,
                        rdf.createIRI("http://example.org/types/Foo"))));
        assertTrue(triples.contains(rdf.createTriple(identifier, type,
                        rdf.createIRI("http://example.org/types/Bar"))));
        assertTrue(triples.contains(rdf.createTriple(identifier, RDFS.label,
                        rdf.createLiteral("A label", "eng"))));
        assertTrue(triples.contains(rdf.createTriple(rdf.createIRI("http://example.org/some/other/resource"),
                    RDFS.label, rdf.createLiteral("Some other resource", "eng"))));

        final List<Triple> inbound = res.stream(FEDORA_INBOUND_REFERENCES).collect(toList());
        assertEquals(3L, inbound.size());
        assertTrue(inbound.contains(rdf.createTriple(rdf.createIRI("info:trellis/resource"),
                        DC.hasPart, identifier)));
        assertTrue(inbound.contains(rdf.createTriple(rdf.createIRI("info:trellis/other/resource"),
                        DC.relation, identifier)));
        assertTrue(inbound.contains(rdf.createTriple(rdf.createIRI("info:trellis/other/item"),
                        DC.hasPart, identifier)));

        final List<VersionRange> mementos = res.getMementos().collect(toList());
        assertEquals(1L, mementos.size());
        assertEquals(parse("2017-02-15T10:05:00Z"), mementos.get(0).getFrom());
        assertEquals(parse("2017-02-15T11:15:00Z"), mementos.get(0).getUntil());
    }
}
