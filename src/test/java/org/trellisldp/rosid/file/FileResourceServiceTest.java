/*
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
package org.trellisldp.rosid.file;

import static org.trellisldp.vocabulary.RDF.type;
import static java.time.Instant.now;
import static java.time.Instant.parse;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.stream.Collectors.toList;
import static org.apache.curator.framework.CuratorFrameworkFactory.newClient;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

import org.apache.commons.rdf.api.Dataset;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.apache.commons.rdf.api.Triple;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.TestingServer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.MockProducer;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.trellisldp.api.Resource;
import org.trellisldp.api.VersionRange;
import org.trellisldp.spi.ResourceService;
import org.trellisldp.rosid.common.DatasetSerialization;
import org.trellisldp.vocabulary.DC;
import org.trellisldp.vocabulary.LDP;
import org.trellisldp.vocabulary.RDFS;
import org.trellisldp.vocabulary.Trellis;

/**
 * @author acoburn
 */
public class FileResourceServiceTest extends BaseRdfTest {

    private static TestingServer zkServer;

    private final IRI identifier = rdf.createIRI("trellis:repository/resource");
    private final IRI other = rdf.createIRI("trellis:repository/other");
    private final IRI testResource = rdf.createIRI("trellis:repository/testResource");
    private final Producer<String, Dataset> mockProducer = new MockProducer<>(true,
            new StringSerializer(), new DatasetSerialization());

    private CuratorFramework curator;
    private ResourceService service;
    private Properties config;

    @BeforeClass
    public static void initialize() throws Exception {
        zkServer = new TestingServer(true);
    }

    @Before
    public void setUp() throws Exception {
        config = new Properties();
        config.setProperty("trellis.storage.repository.resources", getClass().getResource("/root").toURI().toString());
        curator = newClient(zkServer.getConnectString(), new RetryNTimes(10, 1000));
        curator.start();
        service = new FileResourceService(config, curator, mockProducer);
    }

    @Test
    public void testNewRoot() throws IOException {
        final Instant time = parse("2017-02-16T11:15:03Z");
        final Properties configuration = new Properties();
        configuration.setProperty("trellis.storage.repository.resources",
                config.getProperty("trellis.storage.repository.resources") + "/root2/a");
        final File root = new File(URI.create(configuration.getProperty("trellis.storage.repository.resources")));
        assertFalse(root.exists());
        final ResourceService altService = new FileResourceService(configuration, curator, mockProducer);
        assertFalse(altService.get(identifier, time).isPresent());
        assertTrue(root.exists());
        assertFalse(altService.get(identifier, time).isPresent());
    }

    @Test(expected = IOException.class)
    public void testUnwritableRoot() throws IOException {
        final Properties configuration = new Properties();
        configuration.setProperty("trellis.storage.repository.resources",
                config.getProperty("trellis.storage.repository.resources") + "/root3");
        final File root = new File(URI.create(configuration.getProperty("trellis.storage.repository.resources")));
        assertTrue(root.mkdir());
        assertTrue(root.setReadOnly());
        final ResourceService altService = new FileResourceService(configuration, curator, mockProducer);
    }

    @Test
    public void testWriteResource() {
        final Dataset data = rdf.createDataset();
        data.add(rdf.createQuad(Trellis.PreferUserManaged, testResource, DC.title, rdf.createLiteral("A title")));
        data.add(rdf.createQuad(Trellis.PreferServerManaged, testResource, type, LDP.RDFSource));
        assertFalse(service.get(testResource).isPresent());
        assertFalse(service.get(testResource, now()).isPresent());

        assertTrue(service.put(testResource, data));
        final Optional<Resource> res = service.get(testResource, now());
        assertTrue(res.isPresent());
        res.ifPresent(r -> {
            assertEquals(LDP.RDFSource, r.getInteractionModel());
            assertEquals(testResource, r.getIdentifier());
            assertTrue(r.stream().anyMatch(q -> q.getPredicate().equals(DC.title)));
            assertTrue(r.getModified().isBefore(now()));
        });
        final Optional<Resource> res2 = service.get(testResource);
        assertTrue(res2.isPresent());
        res2.ifPresent(r -> {
            assertEquals(LDP.RDFSource, r.getInteractionModel());
            assertEquals(testResource, r.getIdentifier());
            assertTrue(r.stream().anyMatch(q -> q.getPredicate().equals(DC.title)));
            assertTrue(r.getModified().isBefore(now()));
        });
    }

    @Test
    public void testVersionedResource() {
        final Instant time = parse("2017-02-16T11:15:03Z");
        final Resource res = service.get(identifier, time).get();
        assertEquals(identifier, res.getIdentifier());
        assertEquals(LDP.Container, res.getInteractionModel());
        final List<IRI> contained = res.getContains().collect(toList());
        assertEquals(3L, contained.size());
        assertTrue(contained.contains(rdf.createIRI("trellis:repository/resource/1")));
        assertTrue(contained.contains(rdf.createIRI("trellis:repository/resource/2")));
        assertTrue(contained.contains(rdf.createIRI("trellis:repository/resource/3")));
        assertEquals(empty(), res.getMembershipResource());
        assertEquals(empty(), res.getMemberRelation());
        assertEquals(empty(), res.getMemberOfRelation());
        assertEquals(empty(), res.getInsertedContentRelation());
        assertEquals(empty(), res.getBinary());
        assertTrue(res.isMemento());
        assertFalse(res.isPage());
        assertEquals(empty(), res.getNext());
        assertEquals(of(rdf.createIRI("http://example.org/receiver/inbox")), res.getInbox());
        assertEquals(empty(), res.getAcl());
        assertEquals(parse("2017-02-16T11:15:03Z"), res.getModified());
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
    public void testResourceFuture() {
        final Instant time = parse("2017-03-15T11:15:00Z");
        final Resource res = service.get(identifier, time).get();
        assertEquals(identifier, res.getIdentifier());
        assertEquals(LDP.Container, res.getInteractionModel());
        final List<IRI> contained = res.getContains().collect(toList());
        assertEquals(3L, contained.size());
        assertTrue(contained.contains(rdf.createIRI("trellis:repository/resource/1")));
        assertTrue(contained.contains(rdf.createIRI("trellis:repository/resource/2")));
        assertTrue(contained.contains(rdf.createIRI("trellis:repository/resource/3")));
        assertEquals(empty(), res.getMembershipResource());
        assertEquals(empty(), res.getMemberRelation());
        assertEquals(empty(), res.getMemberOfRelation());
        assertEquals(empty(), res.getInsertedContentRelation());
        assertEquals(empty(), res.getBinary());
        assertTrue(res.isMemento());
        assertFalse(res.isPage());
        assertEquals(empty(), res.getNext());
        assertEquals(of(rdf.createIRI("http://example.org/receiver/inbox")), res.getInbox());
        assertEquals(empty(), res.getAcl());
        assertEquals(parse("2017-02-16T11:15:03Z"), res.getModified());
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
    public void testResourcePast() {
        final Instant time = parse("2017-02-15T11:00:00Z");
        final Resource res = service.get(identifier, time).get();
        assertEquals(identifier, res.getIdentifier());
        assertEquals(LDP.Container, res.getInteractionModel());
        assertEquals(empty(), res.getContains().findFirst());
        assertEquals(empty(), res.getMembershipResource());
        assertEquals(empty(), res.getMemberRelation());
        assertEquals(empty(), res.getMemberOfRelation());
        assertEquals(empty(), res.getInsertedContentRelation());
        assertEquals(empty(), res.getBinary());
        assertTrue(res.isMemento());
        assertFalse(res.isPage());
        assertEquals(empty(), res.getNext());
        assertEquals(empty(), res.getInbox());
        assertEquals(empty(), res.getAcl());
        assertEquals(parse("2017-02-15T10:05:00Z"), res.getModified());
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
    public void testResourcePrehistory() {
        final Instant time = parse("2017-01-15T11:00:00Z");
        assertFalse(service.get(identifier, time).isPresent());
    }

    @Test
    public void testCachedResource() {
        final Resource res = service.get(identifier).get();
        assertEquals(identifier, res.getIdentifier());
        assertEquals(LDP.Container, res.getInteractionModel());
        final List<IRI> contained = res.getContains().collect(toList());
        assertEquals(3L, contained.size());
        assertTrue(contained.contains(rdf.createIRI("trellis:repository/resource/1")));
        assertTrue(contained.contains(rdf.createIRI("trellis:repository/resource/2")));
        assertTrue(contained.contains(rdf.createIRI("trellis:repository/resource/3")));
        assertEquals(empty(), res.getMembershipResource());
        assertEquals(empty(), res.getMemberRelation());
        assertEquals(empty(), res.getMemberOfRelation());
        assertEquals(empty(), res.getInsertedContentRelation());
        assertEquals(empty(), res.getBinary());
        assertEquals(empty(), res.getAnnotationService());
        assertFalse(res.isMemento());
        assertFalse(res.isPage());
        assertEquals(empty(), res.getNext());
        assertEquals(of(rdf.createIRI("http://example.org/receiver/inbox")), res.getInbox());
        assertEquals(empty(), res.getAcl());
        assertEquals(parse("2017-02-16T11:15:03Z"), res.getModified());
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
    public void testOtherCachedResource() {
        final Resource res = service.get(other).get();
        assertEquals(other, res.getIdentifier());
        assertEquals(LDP.Container, res.getInteractionModel());
        final List<IRI> contained = res.getContains().collect(toList());
        assertEquals(3L, contained.size());
        assertTrue(contained.contains(rdf.createIRI("trellis:repository/other/1")));
        assertTrue(contained.contains(rdf.createIRI("trellis:repository/other/2")));
        assertTrue(contained.contains(rdf.createIRI("trellis:repository/other/3")));
        assertEquals(empty(), res.getMembershipResource());
        assertEquals(empty(), res.getMemberRelation());
        assertEquals(empty(), res.getMemberOfRelation());
        assertEquals(empty(), res.getInsertedContentRelation());
        assertEquals(empty(), res.getBinary());
        assertEquals(empty(), res.getAnnotationService());
        assertTrue(res.isMemento());
        assertFalse(res.isPage());
        assertEquals(empty(), res.getNext());
        assertEquals(of(rdf.createIRI("http://example.org/receiver/inbox")), res.getInbox());
        assertEquals(empty(), res.getAcl());
        assertEquals(parse("2017-02-16T11:15:03Z"), res.getModified());
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
