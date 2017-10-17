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

import static java.io.File.separator;
import static java.time.Instant.MAX;
import static java.time.Instant.now;
import static java.time.Instant.parse;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;
import static java.util.Optional.of;
import static java.util.stream.Collectors.toList;
import static org.apache.curator.framework.CuratorFrameworkFactory.newClient;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.Assume.assumeTrue;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.trellisldp.vocabulary.RDF.type;
import static org.trellisldp.rosid.file.TestUtils.rdf;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.trellisldp.api.EventService;
import org.trellisldp.api.Resource;
import org.trellisldp.api.ResourceService;
import org.trellisldp.api.VersionRange;
import org.trellisldp.vocabulary.DC;
import org.trellisldp.vocabulary.LDP;
import org.trellisldp.vocabulary.RDFS;
import org.trellisldp.vocabulary.Trellis;
import org.trellisldp.vocabulary.XSD;

/**
 * @author acoburn
 */
@RunWith(JUnitPlatform.class)
public class FileResourceServiceTest {

    private static TestingServer zkServer;

    private final IRI identifier = rdf.createIRI("trellis:repository/resource");
    private final IRI other = rdf.createIRI("trellis:repository/other");
    private final IRI testResource = rdf.createIRI("trellis:repository/testResource");
    private final Producer<String, String> mockProducer = new MockProducer<>(true,
            new StringSerializer(), new StringSerializer());

    private CuratorFramework curator;
    private ResourceService service;
    private Map<String, String> partitions = new HashMap<>();
    private Map<String, String> partitionUrls = new HashMap<>();

    @Mock
    private EventService mockEventService;

    @Mock
    private Supplier<String> mockIdSupplier;

    @BeforeAll
    public static void initialize() throws Exception {
        zkServer = new TestingServer(true);
    }

    @BeforeEach
    public void setUp() throws Exception {
        initMocks(this);
        partitions.clear();
        partitions.put("repository", getClass().getResource("/root").toURI().toString());
        partitionUrls.put("repository", "http://localhost/");
        curator = newClient(zkServer.getConnectString(), new RetryNTimes(10, 1000));
        curator.start();
        service = new FileResourceService(partitions, partitionUrls, curator, mockProducer, mockEventService,
                mockIdSupplier, false);
    }

    @Test
    public void testNewRoot() throws IOException {
        final Instant time = parse("2017-02-16T11:15:03Z");
        final Map<String, String> config = new HashMap<>();
        config.put("repository", partitions.get("repository") + "/root2/a");
        final File root = new File(URI.create(config.get("repository")));
        assertFalse(root.exists());
        final ResourceService altService = new FileResourceService(config, partitionUrls, curator,
                mockProducer, mockEventService, mockIdSupplier, false);
        assertFalse(altService.get(identifier, time).isPresent());
        assertTrue(root.exists());
        assertFalse(altService.get(identifier, time).isPresent());
    }

    @Test
    public void testUnwritableRoot() throws IOException {
        final Map<String, String> config = new HashMap<>();
        config.put("repository", partitions.get("repository") + "/root3");
        final File root = new File(URI.create(config.get("repository")));
        assertTrue(root.mkdir());
        assumeTrue(root.setReadOnly());
        assertThrows(IOException.class, () -> new FileResourceService(config, partitionUrls, curator, mockProducer,
                mockEventService, mockIdSupplier, false));
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
    public void testWriteResourceAsync() throws IOException {
        final ResourceService service = new FileResourceService(partitions, partitionUrls, curator, mockProducer,
                mockEventService, mockIdSupplier, false);
        final IRI testResource2 = rdf.createIRI("trellis:repository/testResource2");
        final Dataset data = rdf.createDataset();
        final IRI inbox1 = rdf.createIRI("http://example.org/1/");
        final IRI inbox2 = rdf.createIRI("http://example.org/2/");
        data.add(rdf.createQuad(Trellis.PreferUserManaged, testResource2, DC.title, rdf.createLiteral("A title")));
        data.add(rdf.createQuad(Trellis.PreferUserManaged, testResource2, LDP.inbox, inbox1));
        data.add(rdf.createQuad(Trellis.PreferServerManaged, testResource2, type, LDP.RDFSource));
        assertFalse(service.get(testResource2).isPresent());
        assertFalse(service.get(testResource2, now()).isPresent());

        assertTrue(service.put(testResource2, data));
        final Optional<Resource> res = service.get(testResource2, now());
        assertTrue(res.isPresent());
        res.ifPresent(r -> {
            assertEquals(LDP.RDFSource, r.getInteractionModel());
            assertEquals(testResource2, r.getIdentifier());
            assertTrue(r.stream().anyMatch(q -> q.getPredicate().equals(DC.title)));
            assertTrue(r.getModified().isBefore(now()));
            assertEquals(of(inbox1), r.getInbox());
        });
        final Optional<Resource> res2 = service.get(testResource2);
        assertTrue(res2.isPresent());
        res2.ifPresent(r -> {
            assertEquals(LDP.RDFSource, r.getInteractionModel());
            assertEquals(testResource2, r.getIdentifier());
            assertTrue(r.stream().anyMatch(q -> q.getPredicate().equals(DC.title)));
            assertTrue(r.getModified().isBefore(now()));
            assertEquals(of(inbox1), r.getInbox());
        });

        final ResourceService service2 = new FileResourceService(partitions, partitionUrls, curator, mockProducer,
                mockEventService, mockIdSupplier, true);

        final Dataset data2 = rdf.createDataset();
        data.stream().filter(q -> !LDP.inbox.equals(q.getPredicate())).forEach(data2::add);
        data2.add(rdf.createQuad(Trellis.PreferUserManaged, testResource2, LDP.inbox, inbox2));
        assertTrue(service2.put(testResource2, data2));
        final Optional<Resource> res3 = service2.get(testResource2, MAX);
        assertTrue(res3.isPresent());
        res3.ifPresent(r -> {
            assertEquals(LDP.RDFSource, r.getInteractionModel());
            assertEquals(testResource2, r.getIdentifier());
            assertTrue(r.stream().anyMatch(q -> q.getPredicate().equals(DC.title)));
            assertTrue(r.stream().anyMatch(q -> q.getPredicate().equals(LDP.inbox)));
            assertTrue(r.getModified().isBefore(now()));
            assertEquals(of(inbox2), r.getInbox());
        });

        final Optional<Resource> res4 = service2.get(testResource2);
        assertTrue(res4.isPresent());
        res4.ifPresent(r -> {
            assertEquals(LDP.RDFSource, r.getInteractionModel());
            assertEquals(testResource2, r.getIdentifier());
            assertTrue(r.stream().anyMatch(q -> q.getPredicate().equals(DC.title)));
            assertTrue(r.stream().anyMatch(q -> q.getPredicate().equals(LDP.inbox)));
            assertTrue(r.getModified().isBefore(now()));
            assertEquals(of(inbox1), r.getInbox());
        });
    }

    @Test
    public void testWriteResourceWrongLocation() {
        final Dataset data = rdf.createDataset();
        data.add(rdf.createQuad(Trellis.PreferUserManaged, testResource, DC.title, rdf.createLiteral("A title")));
        data.add(rdf.createQuad(Trellis.PreferServerManaged, testResource, type, LDP.RDFSource));

        assertFalse(service.put(rdf.createIRI("trellis:foo/bar"), data));
    }

    @Test
    public void testVersionedResource() {
        final Instant time = parse("2017-02-16T11:15:03Z");
        final Resource res = service.get(identifier, time).get();
        assertEquals(identifier, res.getIdentifier());
        assertEquals(LDP.Container, res.getInteractionModel());
        final List<IRI> contained = res.stream(LDP.PreferContainment).map(Triple::getObject).map(x -> (IRI)x)
            .collect(toList());
        assertEquals(3L, contained.size());
        assertTrue(contained.contains(rdf.createIRI("trellis:repository/resource/1")));
        assertTrue(contained.contains(rdf.createIRI("trellis:repository/resource/2")));
        assertTrue(contained.contains(rdf.createIRI("trellis:repository/resource/3")));
        assertFalse(res.getMembershipResource().isPresent());
        assertFalse(res.getMemberRelation().isPresent());
        assertFalse(res.getMemberOfRelation().isPresent());
        assertFalse(res.getInsertedContentRelation().isPresent());
        assertFalse(res.getBinary().isPresent());
        assertTrue(res.isMemento());
        assertEquals(of(rdf.createIRI("http://example.org/receiver/inbox")), res.getInbox());
        assertEquals(parse("2017-02-16T11:15:03Z"), res.getModified());
        assertEquals(2L, res.getTypes().size());
        assertTrue(res.getTypes().contains(rdf.createIRI("http://example.org/types/Foo")));
        assertTrue(res.getTypes().contains(rdf.createIRI("http://example.org/types/Bar")));
        assertEquals(3L, res.stream().filter(TestUtils.isContainment).count());
        assertEquals(0L, res.stream().filter(TestUtils.isMembership).count());

        final List<VersionRange> mementos = res.getMementos();
        assertEquals(1L, mementos.size());
        assertEquals(parse("2017-02-15T10:05:00Z"), mementos.get(0).getFrom());
        assertEquals(parse("2017-02-15T11:15:00Z"), mementos.get(0).getUntil());

        final List<Triple> triples = res.stream().filter(TestUtils.isUserManaged)
            .map(Quad::asTriple).collect(toList());
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
    }

    @Test
    public void testResourceFuture() {
        final Instant time = parse("2017-03-15T11:15:00Z");
        final Resource res = service.get(identifier, time).get();
        assertEquals(identifier, res.getIdentifier());
        assertEquals(LDP.Container, res.getInteractionModel());
        final List<IRI> contained = res.stream(LDP.PreferContainment).map(Triple::getObject).map(x -> (IRI)x)
            .collect(toList());
        assertEquals(3L, contained.size());
        assertTrue(contained.contains(rdf.createIRI("trellis:repository/resource/1")));
        assertTrue(contained.contains(rdf.createIRI("trellis:repository/resource/2")));
        assertTrue(contained.contains(rdf.createIRI("trellis:repository/resource/3")));
        assertFalse(res.getMembershipResource().isPresent());
        assertFalse(res.getMemberRelation().isPresent());
        assertFalse(res.getMemberOfRelation().isPresent());
        assertFalse(res.getInsertedContentRelation().isPresent());
        assertFalse(res.getBinary().isPresent());
        assertTrue(res.isMemento());
        assertEquals(of(rdf.createIRI("http://example.org/receiver/inbox")), res.getInbox());
        assertEquals(parse("2017-02-16T11:15:03Z"), res.getModified());
        assertEquals(2L, res.getTypes().size());
        assertTrue(res.getTypes().contains(rdf.createIRI("http://example.org/types/Foo")));
        assertTrue(res.getTypes().contains(rdf.createIRI("http://example.org/types/Bar")));
        assertEquals(3L, res.stream().filter(TestUtils.isContainment).count());
        assertEquals(0L, res.stream().filter(TestUtils.isMembership).count());

        final List<Triple> triples = res.stream().filter(TestUtils.isUserManaged)
            .map(Quad::asTriple).collect(toList());
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

        final List<VersionRange> mementos = res.getMementos();
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
        assertFalse(res.getMembershipResource().isPresent());
        assertFalse(res.getMemberRelation().isPresent());
        assertFalse(res.getMemberOfRelation().isPresent());
        assertFalse(res.getInsertedContentRelation().isPresent());
        assertFalse(res.getBinary().isPresent());
        assertTrue(res.isMemento());
        assertFalse(res.getInbox().isPresent());
        assertEquals(parse("2017-02-15T10:05:00Z"), res.getModified());
        assertEquals(0L, res.getTypes().size());
        assertEquals(0L, res.stream().filter(TestUtils.isContainment.or(TestUtils.isMembership)).count());

        final List<Triple> triples = res.stream().filter(TestUtils.isUserManaged)
            .map(Quad::asTriple).collect(toList());
        assertEquals(0L, triples.size());

        final List<VersionRange> mementos = res.getMementos();
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
        final List<IRI> contained = res.stream(LDP.PreferContainment).map(Triple::getObject).map(x -> (IRI)x)
            .collect(toList());
        assertEquals(3L, contained.size());
        assertTrue(contained.contains(rdf.createIRI("trellis:repository/resource/1")));
        assertTrue(contained.contains(rdf.createIRI("trellis:repository/resource/2")));
        assertTrue(contained.contains(rdf.createIRI("trellis:repository/resource/3")));
        assertFalse(res.getMembershipResource().isPresent());
        assertFalse(res.getMemberRelation().isPresent());
        assertFalse(res.getMemberOfRelation().isPresent());
        assertFalse(res.getInsertedContentRelation().isPresent());
        assertFalse(res.getBinary().isPresent());
        assertFalse(res.getAnnotationService().isPresent());
        assertFalse(res.isMemento());
        assertEquals(of(rdf.createIRI("http://example.org/receiver/inbox")), res.getInbox());
        assertEquals(parse("2017-02-16T11:15:03Z"), res.getModified());
        assertEquals(2L, res.getTypes().size());
        assertTrue(res.getTypes().contains(rdf.createIRI("http://example.org/types/Foo")));
        assertTrue(res.getTypes().contains(rdf.createIRI("http://example.org/types/Bar")));
        assertEquals(3L, res.stream().filter(TestUtils.isContainment).count());
        assertEquals(0L, res.stream().filter(TestUtils.isMembership).count());

        final List<Triple> triples = res.stream().filter(TestUtils.isUserManaged)
            .map(Quad::asTriple).collect(toList());
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

        final List<VersionRange> mementos = res.getMementos();
        assertEquals(1L, mementos.size());
        assertEquals(parse("2017-02-15T10:05:00Z"), mementos.get(0).getFrom());
        assertEquals(parse("2017-02-15T11:15:00Z"), mementos.get(0).getUntil());
    }

    @Test
    public void testOtherCachedResource() {
        final Resource res = service.get(other).get();
        assertEquals(other, res.getIdentifier());
        assertEquals(LDP.Container, res.getInteractionModel());
        final List<IRI> contained = res.stream(LDP.PreferContainment).map(Triple::getObject).map(x -> (IRI)x)
            .collect(toList());
        assertEquals(3L, contained.size());
        assertTrue(contained.contains(rdf.createIRI("trellis:repository/other/1")));
        assertTrue(contained.contains(rdf.createIRI("trellis:repository/other/2")));
        assertTrue(contained.contains(rdf.createIRI("trellis:repository/other/3")));
        assertFalse(res.getMembershipResource().isPresent());
        assertFalse(res.getMemberRelation().isPresent());
        assertFalse(res.getMemberOfRelation().isPresent());
        assertFalse(res.getInsertedContentRelation().isPresent());
        assertFalse(res.getBinary().isPresent());
        assertFalse(res.getAnnotationService().isPresent());
        assertTrue(res.isMemento());
        assertEquals(of(rdf.createIRI("http://example.org/receiver/inbox")), res.getInbox());
        assertEquals(parse("2017-02-16T11:15:03Z"), res.getModified());
        assertEquals(2L, res.getTypes().size());
        assertTrue(res.getTypes().contains(rdf.createIRI("http://example.org/types/Foo")));
        assertTrue(res.getTypes().contains(rdf.createIRI("http://example.org/types/Bar")));
        assertEquals(3L, res.stream().filter(TestUtils.isContainment).count());
        assertEquals(0L, res.stream().filter(TestUtils.isMembership).count());

        final List<Triple> triples = res.stream().filter(TestUtils.isUserManaged)
            .map(Quad::asTriple).collect(toList());
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

        final List<VersionRange> mementos = res.getMementos();
        assertEquals(1L, mementos.size());
        assertEquals(parse("2017-02-15T10:05:00Z"), mementos.get(0).getFrom());
        assertEquals(parse("2017-02-15T11:15:00Z"), mementos.get(0).getUntil());
    }

    @Test
    public void testScan() throws Exception {
        final String path = new File(getClass().getResource("/rootList").toURI()).getAbsolutePath();
        partitions.put("repository", path);
        final List<Triple> triples = service.scan("repository").collect(toList());
        assertEquals(3L, triples.size());
        assertTrue(triples.contains(rdf.createTriple(testResource, type, LDP.RDFSource)));
        assertTrue(triples.contains(rdf.createTriple(identifier, type, LDP.Container)));
        assertTrue(triples.contains(rdf.createTriple(rdf.createIRI("trellis:repository"), type, LDP.Container)));
    }

    @Test
    public void testExport() throws Exception {
        final String path = new File(getClass().getResource("/rootList").toURI()).getAbsolutePath();
        final IRI root = rdf.createIRI("trellis:repository");
        partitions.put("repository", path);
        final List<Quad> quads = service.export("repository", singleton(Trellis.PreferUserManaged)).collect(toList());
        assertEquals(6L, quads.size());
        assertTrue(quads.contains(rdf.createQuad(testResource, testResource, DC.title, rdf.createLiteral("A title"))));
        assertTrue(quads.contains(rdf.createQuad(identifier, identifier, LDP.inbox,
                        rdf.createIRI("http://example.org/receiver/inbox"))));
        assertTrue(quads.contains(rdf.createQuad(identifier, identifier, type,
                        rdf.createIRI("http://example.org/types/Foo"))));
        assertTrue(quads.contains(rdf.createQuad(identifier, identifier, type,
                        rdf.createIRI("http://example.org/types/Bar"))));
        assertTrue(quads.contains(rdf.createQuad(identifier, identifier, RDFS.label,
                        rdf.createLiteral("A label", "eng"))));
        assertTrue(quads.contains(rdf.createQuad(identifier, rdf.createIRI("http://example.org/some/other/resource"),
                        RDFS.label, rdf.createLiteral("Some other resource", "eng"))));

        // Test server managed expport
        final List<Quad> otherQuads = service.export("repository", singleton(Trellis.PreferServerManaged))
            .collect(toList());
        assertEquals(6L, otherQuads.size());
        assertTrue(otherQuads.contains(rdf.createQuad(testResource, testResource, type, LDP.RDFSource)));
        assertTrue(otherQuads.contains(rdf.createQuad(testResource, testResource, DC.modified,
                        rdf.createLiteral("2017-09-05T13:49:58.417Z", XSD.dateTime))));
        assertTrue(otherQuads.contains(rdf.createQuad(root, root, type, LDP.Container)));
        assertTrue(otherQuads.contains(rdf.createQuad(root, root, DC.modified,
                        rdf.createLiteral("2017-09-05T13:49:58.012Z", XSD.dateTime))));
        assertTrue(otherQuads.contains(rdf.createQuad(identifier, identifier, type, LDP.Container)));
        assertTrue(otherQuads.contains(rdf.createQuad(identifier, identifier, DC.modified,
                        rdf.createLiteral("2017-02-16T11:15:03Z", XSD.dateTime))));
    }

    @Test
    public void testListNoPartition() {
        assertEquals(0L, service.scan("non-existent").count());
    }

    @Test
    public void testListInvalidPath() throws Exception {
        final Map<String, String> myPartitions = singletonMap("foo",
                new File(getClass().getResource("/rootList").toURI()).getAbsolutePath() + separator + "non-existent");
        final Map<String, String> myUrls = singletonMap("foo", "http://localhost/");
        service = new FileResourceService(myPartitions, myUrls, curator, mockProducer, mockEventService,
                mockIdSupplier, false);
        assertEquals(1L, service.scan("foo").count());
        assertEquals(of(rdf.createTriple(rdf.createIRI("trellis:foo"), type, LDP.Container)),
                service.scan("foo").findFirst());
        assertEquals(0L, service.scan("error").count());
    }

    @Test
    public void testPurge() throws Exception {
        final Map<String, String> myPartitions = singletonMap("repository",
                new File(getClass().getResource("/purgeable").toURI()).getAbsolutePath());
        service = new FileResourceService(myPartitions, partitionUrls, curator, mockProducer, mockEventService,
                mockIdSupplier, false);
        assertTrue(service.get(identifier).isPresent());
        final List<IRI> binaries = service.purge(identifier).collect(toList());
        assertEquals(1L, binaries.size());
        assertEquals(rdf.createIRI("s3://bucket/some-resource"), binaries.get(0));

        assertFalse(service.get(identifier).isPresent());

        assertTrue(service.get(testResource).isPresent());
        assertEquals(0L, service.purge(testResource).count());
        assertFalse(service.get(testResource).isPresent());
    }

    @Test
    public void testInvalidPartitionName() throws Exception {
        final Map<String, String> myPartitions = singletonMap("admin",
                new File(getClass().getResource("/rootList").toURI()).getAbsolutePath());
        assertThrows(IllegalArgumentException.class, () -> new FileResourceService(myPartitions, partitionUrls, curator,
                    mockProducer, mockEventService, mockIdSupplier, false));
    }

    @Test
    public void testCompact() {
        assertThrows(UnsupportedOperationException.class, () -> service.compact(identifier, now(), now()));
    }
}
