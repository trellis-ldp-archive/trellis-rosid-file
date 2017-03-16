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
package edu.amherst.acdc.trellis.rosid.file;

import static edu.amherst.acdc.trellis.vocabulary.RDF.type;
import static java.time.Instant.parse;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.time.Instant;
import java.util.List;

import edu.amherst.acdc.trellis.api.Resource;
import edu.amherst.acdc.trellis.api.VersionRange;
import edu.amherst.acdc.trellis.spi.EventService;
import edu.amherst.acdc.trellis.spi.ResourceService;
import edu.amherst.acdc.trellis.spi.Session;
import edu.amherst.acdc.trellis.rosid.common.DatasetSerialization;
import edu.amherst.acdc.trellis.vocabulary.DC;
import edu.amherst.acdc.trellis.vocabulary.LDP;
import edu.amherst.acdc.trellis.vocabulary.RDFS;
import org.apache.commons.rdf.api.Dataset;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.apache.commons.rdf.api.Triple;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.streams.KafkaStreams;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

/**
 * @author acoburn
 */
@RunWith(MockitoJUnitRunner.class)
public class FileResourceServiceTest extends BaseRdfTest {

    private final IRI identifier = rdf.createIRI("trellis:repository/resource");
    private final IRI other = rdf.createIRI("trellis:repository/other");
    private final Producer<String, Dataset> mockProducer = new MockProducer<>(true,
            new StringSerializer(), new DatasetSerialization());

    private ResourceService service;
    private Configuration config;

    @Mock
    private Session mockSession;

    @Mock
    private EventService mockEventService, mockEventService2;

    @Mock
    private KafkaStreams mockStreams;

    @Before
    public void setUp() throws Exception {
        config = new Configuration();
        config.storage.put("repository", getClass().getResource("/root").toURI().toString());
        service = new FileResourceService(config, mockProducer, mockStreams);
    }

    @Test
    public void testNewRoot() throws IOException {
        final Instant time = parse("2017-02-16T11:15:03Z");
        final Configuration configuration = new Configuration();
        configuration.storage.put("repository", config.storage.get("repository") + "/root2/a");
        final File root = new File(URI.create(configuration.storage.get("repository")));
        assertFalse(root.exists());
        final ResourceService altService = new FileResourceService(configuration, mockProducer, mockStreams);
        assertFalse(altService.exists(mockSession, identifier, time));
        assertTrue(root.exists());
        altService.bind(mockEventService);
        altService.unbind(mockEventService);
        altService.bind(mockEventService);
        altService.unbind(mockEventService2);
        assertFalse(altService.exists(mockSession, identifier, time));
    }

    @Test(expected = IOException.class)
    public void testUnwritableRoot() throws IOException {
        final Configuration configuration = new Configuration();
        configuration.storage.put("repository", config.storage.get("repository") + "/root3");
        final File root = new File(URI.create(configuration.storage.get("repository")));
        assertTrue(root.mkdir());
        assertTrue(root.setReadOnly());
        final ResourceService altService = new FileResourceService(configuration, mockProducer, mockStreams);
    }

    @Test
    public void testVersionedResourceExists() {
        final Instant time = parse("2017-02-16T11:15:03Z");
        assertTrue(service.exists(mockSession, identifier, time));
    }

    @Test
    public void testVersionedResource() {
        final Instant time = parse("2017-02-16T11:15:03Z");
        final Resource res = service.get(mockSession, identifier, time).get();
        assertEquals(identifier, res.getIdentifier());
        assertEquals(LDP.Container, res.getInteractionModel());
        assertEquals(of(rdf.createIRI("trellis:repository")), res.getContainedBy());
        final List<IRI> contained = res.getContains().collect(toList());
        assertEquals(3L, contained.size());
        assertTrue(contained.contains(rdf.createIRI("trellis:repository/resource/1")));
        assertTrue(contained.contains(rdf.createIRI("trellis:repository/resource/2")));
        assertTrue(contained.contains(rdf.createIRI("trellis:repository/resource/3")));
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
        assertEquals(3L, res.stream().filter(isContainment).count());
        assertEquals(0L, res.stream().filter(isMembership).count());

        final List<VersionRange> mementos = res.getMementos().collect(toList());
        assertEquals(1L, mementos.size());
        assertEquals(parse("2017-02-15T10:05:00Z"), mementos.get(0).getFrom());
        assertEquals(parse("2017-02-15T11:15:00Z"), mementos.get(0).getUntil());

        final List<Triple> triples = res.stream().filter(isUserManaged).map(Quad::asTriple).collect(toList());
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

        final List<Triple> inbound = res.stream().filter(isInbound).map(Quad::asTriple).collect(toList());
        assertEquals(2L, inbound.size());
        assertTrue(inbound.contains(rdf.createTriple(rdf.createIRI("trellis:repository/other"),
                        DC.hasPart, identifier)));
        assertTrue(inbound.contains(rdf.createTriple(rdf.createIRI("trellis:repository/other/resource"),
                        DC.relation, identifier)));
    }

    @Test
    public void testFutureResourceExists() {
        final Instant time = parse("2017-03-15T11:15:00Z");
        assertTrue(service.exists(mockSession, identifier, time));
    }

    @Test
    public void testResourceFuture() {
        final Instant time = parse("2017-03-15T11:15:00Z");
        final Resource res = service.get(mockSession, identifier, time).get();
        assertEquals(identifier, res.getIdentifier());
        assertEquals(LDP.Container, res.getInteractionModel());
        assertEquals(of(rdf.createIRI("trellis:repository")), res.getContainedBy());
        final List<IRI> contained = res.getContains().collect(toList());
        assertEquals(3L, contained.size());
        assertTrue(contained.contains(rdf.createIRI("trellis:repository/resource/1")));
        assertTrue(contained.contains(rdf.createIRI("trellis:repository/resource/2")));
        assertTrue(contained.contains(rdf.createIRI("trellis:repository/resource/3")));
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
        assertEquals(3L, res.stream().filter(isContainment).count());
        assertEquals(0L, res.stream().filter(isMembership).count());

        final List<Triple> triples = res.stream().filter(isUserManaged).map(Quad::asTriple).collect(toList());
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

        final List<Triple> inbound = res.stream().filter(isInbound).map(Quad::asTriple).collect(toList());
        assertEquals(3L, inbound.size());
        assertTrue(inbound.contains(rdf.createTriple(rdf.createIRI("trellis:repository/other"),
                        DC.hasPart, identifier)));
        assertTrue(inbound.contains(rdf.createTriple(rdf.createIRI("trellis:repository/other/resource"),
                        DC.relation, identifier)));
        assertTrue(inbound.contains(rdf.createTriple(rdf.createIRI("trellis:repository/other/item"),
                        DC.hasPart, identifier)));

        final List<VersionRange> mementos = res.getMementos().collect(toList());
        assertEquals(1L, mementos.size());
        assertEquals(parse("2017-02-15T10:05:00Z"), mementos.get(0).getFrom());
        assertEquals(parse("2017-02-15T11:15:00Z"), mementos.get(0).getUntil());
    }

    @Test
    public void testPastResourceExists() {
        final Instant time = parse("2017-02-15T11:00:00Z");
        assertTrue(service.exists(mockSession, identifier, time));
    }

    @Test
    public void testResourcePast() {
        final Instant time = parse("2017-02-15T11:00:00Z");
        final Resource res = service.get(mockSession, identifier, time).get();
        assertEquals(identifier, res.getIdentifier());
        assertEquals(LDP.Container, res.getInteractionModel());
        assertEquals(of(rdf.createIRI("trellis:repository")), res.getContainedBy());
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
        assertEquals(0L, res.stream().filter(isContainment.or(isMembership)).count());

        final List<Triple> triples = res.stream().filter(isUserManaged).map(Quad::asTriple).collect(toList());
        assertEquals(0L, triples.size());

        final List<Triple> inbound = res.stream().filter(isInbound).map(Quad::asTriple).collect(toList());
        assertEquals(2L, inbound.size());
        assertTrue(inbound.contains(rdf.createTriple(rdf.createIRI("trellis:repository/other"),
                        DC.hasPart, identifier)));
        assertTrue(inbound.contains(rdf.createTriple(rdf.createIRI("trellis:repository/other/resource"),
                        DC.relation, identifier)));

        final List<VersionRange> mementos = res.getMementos().collect(toList());
        assertEquals(1L, mementos.size());
        assertEquals(parse("2017-02-15T10:05:00Z"), mementos.get(0).getFrom());
        assertEquals(parse("2017-02-15T11:15:00Z"), mementos.get(0).getUntil());
    }

    @Test
    public void testPrehistoryExistence() {
        final Instant time = parse("2017-01-15T11:00:00Z");
        assertFalse(service.exists(mockSession, identifier, time));
    }

    @Test
    public void testResourcePrehistory() {
        final Instant time = parse("2017-01-15T11:00:00Z");
        assertFalse(service.get(mockSession, identifier, time).isPresent());
    }

    @Test
    public void testCachedResourceExists() {
        assertTrue(service.exists(mockSession, identifier));
    }

    @Test
    public void testCachedResource() {
        final Resource res = service.get(mockSession, identifier).get();
        assertEquals(identifier, res.getIdentifier());
        assertEquals(LDP.Container, res.getInteractionModel());
        assertEquals(of(rdf.createIRI("trellis:repository")), res.getContainedBy());
        final List<IRI> contained = res.getContains().collect(toList());
        assertEquals(3L, contained.size());
        assertTrue(contained.contains(rdf.createIRI("trellis:repository/resource/1")));
        assertTrue(contained.contains(rdf.createIRI("trellis:repository/resource/2")));
        assertTrue(contained.contains(rdf.createIRI("trellis:repository/resource/3")));
        assertEquals(empty(), res.getMembershipResource());
        assertEquals(empty(), res.getMemberRelation());
        assertEquals(empty(), res.getMemberOfRelation());
        assertEquals(empty(), res.getInsertedContentRelation());
        assertEquals(empty(), res.getDatastream());
        assertEquals(empty(), res.getAnnotationService());
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
        assertEquals(3L, res.stream().filter(isContainment).count());
        assertEquals(0L, res.stream().filter(isMembership).count());

        final List<Triple> triples = res.stream().filter(isUserManaged).map(Quad::asTriple).collect(toList());
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

        final List<Triple> inbound = res.stream().filter(isInbound).map(Quad::asTriple).collect(toList());
        assertEquals(3L, inbound.size());
        assertTrue(inbound.contains(rdf.createTriple(rdf.createIRI("trellis:repository/other"),
                        DC.hasPart, identifier)));
        assertTrue(inbound.contains(rdf.createTriple(rdf.createIRI("trellis:repository/other/resource"),
                        DC.relation, identifier)));
        assertTrue(inbound.contains(rdf.createTriple(rdf.createIRI("trellis:repository/other/item"),
                        DC.hasPart, identifier)));

        final List<VersionRange> mementos = res.getMementos().collect(toList());
        assertEquals(1L, mementos.size());
        assertEquals(parse("2017-02-15T10:05:00Z"), mementos.get(0).getFrom());
        assertEquals(parse("2017-02-15T11:15:00Z"), mementos.get(0).getUntil());
    }

    @Test
    public void testOtherCachedResourceExists() {
        assertTrue(service.exists(mockSession, other));
    }

    @Test
    public void testOtherCachedResource() {
        final Resource res = service.get(mockSession, other).get();
        assertEquals(other, res.getIdentifier());
        assertEquals(LDP.Container, res.getInteractionModel());
        assertEquals(of(rdf.createIRI("trellis:repository")), res.getContainedBy());
        final List<IRI> contained = res.getContains().collect(toList());
        assertEquals(3L, contained.size());
        assertTrue(contained.contains(rdf.createIRI("trellis:repository/other/1")));
        assertTrue(contained.contains(rdf.createIRI("trellis:repository/other/2")));
        assertTrue(contained.contains(rdf.createIRI("trellis:repository/other/3")));
        assertEquals(empty(), res.getMembershipResource());
        assertEquals(empty(), res.getMemberRelation());
        assertEquals(empty(), res.getMemberOfRelation());
        assertEquals(empty(), res.getInsertedContentRelation());
        assertEquals(empty(), res.getDatastream());
        assertEquals(empty(), res.getAnnotationService());
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
        assertEquals(3L, res.stream().filter(isContainment).count());
        assertEquals(0L, res.stream().filter(isMembership).count());

        final List<Triple> triples = res.stream().filter(isUserManaged).map(Quad::asTriple).collect(toList());
        assertEquals(5L, triples.size());
        assertTrue(triples.contains(rdf.createTriple(other, LDP.inbox,
                        rdf.createIRI("http://example.org/receiver/inbox"))));
        assertTrue(triples.contains(rdf.createTriple(other, type,
                        rdf.createIRI("http://example.org/types/Foo"))));
        assertTrue(triples.contains(rdf.createTriple(other, type,
                        rdf.createIRI("http://example.org/types/Bar"))));
        assertTrue(triples.contains(rdf.createTriple(other, RDFS.label,
                        rdf.createLiteral("A label", "eng"))));
        assertTrue(triples.contains(rdf.createTriple(rdf.createIRI("http://example.org/some/other/resource"),
                    RDFS.label, rdf.createLiteral("Some other resource", "eng"))));

        final List<Triple> inbound = res.stream().filter(isInbound).map(Quad::asTriple).collect(toList());
        assertEquals(3L, inbound.size());
        assertTrue(inbound.contains(rdf.createTriple(rdf.createIRI("trellis:repository/resource"),
                        DC.hasPart, other)));
        assertTrue(inbound.contains(rdf.createTriple(rdf.createIRI("trellis:repository/other/resource"),
                        DC.relation, other)));
        assertTrue(inbound.contains(rdf.createTriple(rdf.createIRI("trellis:repository/other/item"),
                        DC.hasPart, other)));

        final List<VersionRange> mementos = res.getMementos().collect(toList());
        assertEquals(1L, mementos.size());
        assertEquals(parse("2017-02-15T10:05:00Z"), mementos.get(0).getFrom());
        assertEquals(parse("2017-02-15T11:15:00Z"), mementos.get(0).getUntil());
    }
}
