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
import static java.time.Instant.parse;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.trellisldp.api.Resource;
import org.trellisldp.api.VersionRange;
import org.trellisldp.vocabulary.DC;
import org.trellisldp.vocabulary.LDP;
import org.trellisldp.vocabulary.RDFS;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.apache.commons.rdf.api.Triple;
import org.junit.Before;
import org.junit.Test;

/**
 * @author acoburn
 */
public class ResourceWriterTest extends BaseRdfTest {

    private static final IRI identifier = rdf.createIRI("trellis:repository/resource");
    private File directory2 = null;
    private File directory4 = null;

    @Before
    public void setUp() throws Exception {
        directory2 = new File(getClass().getResource("/res2").toURI());
        directory4 = new File(getClass().getResource("/res4").toURI());
    }

    @Test
    public void testCacheWriter1() throws IOException {
        final Instant time = parse("2017-03-15T01:23:45Z");
        CachedResource.write(directory2, identifier, time);
        final Optional<Resource> resource = CachedResource.find(directory2, identifier);
        assertTrue(resource.isPresent());
        resource.ifPresent(res -> {
            assertEquals(identifier, res.getIdentifier());
            assertEquals(LDP.RDFSource, res.getInteractionModel());
            assertEquals(empty(), res.getContains().findFirst());
            assertEquals(empty(), res.getMembershipResource());
            assertEquals(empty(), res.getMemberRelation());
            assertEquals(empty(), res.getMemberOfRelation());
            assertEquals(empty(), res.getInsertedContentRelation());
            assertEquals(empty(), res.getBlob());
            assertFalse(res.isMemento());
            assertFalse(res.isPage());
            assertEquals(empty(), res.getNext());
            assertEquals(of(rdf.createIRI("http://example.org/receiver/inbox")), res.getInbox());
            assertEquals(empty(), res.getAcl());
            assertEquals(parse("2017-03-03T02:34:12Z"), res.getModified());
            assertEquals(2L, res.getTypes().count());
            assertTrue(res.getTypes().anyMatch(rdf.createIRI("http://example.org/types/Foo")::equals));
            assertTrue(res.getTypes().anyMatch(rdf.createIRI("http://example.org/types/Bar")::equals));
            assertEquals(0L, res.stream().filter(isContainment.or(isMembership)).count());

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
            assertEquals(3L, mementos.size());
            assertEquals(parse("2017-02-15T10:05:00Z"), mementos.get(0).getFrom());
            assertEquals(parse("2017-02-15T11:15:00Z"), mementos.get(0).getUntil());
            assertEquals(parse("2017-02-15T11:15:00Z"), mementos.get(1).getFrom());
            assertEquals(parse("2017-03-02T02:34:12Z"), mementos.get(1).getUntil());
            assertEquals(parse("2017-03-02T02:34:12Z"), mementos.get(2).getFrom());
            assertEquals(parse("2017-03-03T02:34:12Z"), mementos.get(2).getUntil());
        });
    }

    @Test
    public void testCacheWriter2() throws IOException {
        CachedResource.write(directory4, identifier);
        final Optional<Resource> resource = CachedResource.find(directory4, identifier);
        assertTrue(resource.isPresent());
        resource.ifPresent(res -> {
            assertEquals(identifier, res.getIdentifier());
            assertEquals(LDP.RDFSource, res.getInteractionModel());
            assertEquals(empty(), res.getContains().findFirst());
            assertEquals(empty(), res.getMembershipResource());
            assertEquals(empty(), res.getMemberRelation());
            assertEquals(empty(), res.getMemberOfRelation());
            assertEquals(empty(), res.getInsertedContentRelation());
            assertEquals(empty(), res.getBlob());
            assertFalse(res.isMemento());
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
            assertEquals(1L, inbound.size());
            assertTrue(inbound.contains(rdf.createTriple(rdf.createIRI("trellis:repository/other/item"),
                            DC.hasPart, identifier)));

            final List<VersionRange> mementos = res.getMementos().collect(toList());
            assertEquals(0L, mementos.size());
        });
    }
}
