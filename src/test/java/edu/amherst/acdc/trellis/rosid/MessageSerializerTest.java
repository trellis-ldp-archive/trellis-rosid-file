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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import edu.amherst.acdc.trellis.vocabulary.DC;
import edu.amherst.acdc.trellis.vocabulary.LDP;

import org.apache.commons.rdf.api.Graph;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Triple;
import org.apache.commons.rdf.api.RDF;
import org.apache.commons.rdf.jena.JenaRDF;
import org.junit.Before;
import org.junit.Test;

/**
 * @author acoburn
 */
public class MessageSerializerTest {

    private static final RDF rdf = new JenaRDF();

    private final MessageSerializer serializer = new MessageSerializer();
    private final IRI identifier = rdf.createIRI("info:trellis/resource");
    private final Triple title = rdf.createTriple(identifier, DC.title, rdf.createLiteral("A title", "eng"));
    private final Triple description = rdf.createTriple(identifier, DC.description,
            rdf.createLiteral("A longer description", "eng"));
    private final Triple subject = rdf.createTriple(identifier, DC.subject,
            rdf.createIRI("http://example.org/subject/1"));

    private final Graph graph = rdf.createGraph();

    @Before
    public void setUp() {
        graph.clear();
        graph.add(title);
        graph.add(description);
        graph.add(subject);
    }

    @Test
    public void testSerialization() {
        final Message msg = new Message(identifier, LDP.Container, graph);
        final Message msg2 = serializer.deserialize("topic", serializer.serialize("topic", msg));
        assertEquals(identifier, msg2.getIdentifier());
        assertEquals(LDP.Container, msg2.getModel());
        assertTrue(msg2.getGraph().contains(title));
        assertTrue(msg2.getGraph().contains(description));
        assertTrue(msg2.getGraph().contains(subject));
        assertEquals(3L, msg2.getGraph().size());
    }

    @Test
    public void testDeserialization() {
        final String data = "info:trellis/resource,http://www.w3.org/ns/ldp#Container," +
            "<info:trellis/resource> <http://purl.org/dc/terms/title> \"A title\"@eng .\n" +
            "<info:trellis/resource> <http://purl.org/dc/terms/description> \"A longer description\"@eng .\n" +
            "<info:trellis/resource> <http://purl.org/dc/terms/subject> <http://example.org/subject/1> .\n";
        final Message msg = serializer.deserialize("topic", data.getBytes(UTF_8));
        assertEquals(identifier, msg.getIdentifier());
        assertEquals(LDP.Container, msg.getModel());
        assertTrue(msg.getGraph().contains(title));
        assertTrue(msg.getGraph().contains(description));
        assertTrue(msg.getGraph().contains(subject));
        assertEquals(3L, msg.getGraph().size());

    }
}
