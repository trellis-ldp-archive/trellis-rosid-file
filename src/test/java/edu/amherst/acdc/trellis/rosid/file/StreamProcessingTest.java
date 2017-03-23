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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import edu.amherst.acdc.trellis.vocabulary.DC;
import edu.amherst.acdc.trellis.vocabulary.LDP;
import edu.amherst.acdc.trellis.vocabulary.Trellis;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.rdf.api.Dataset;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.apache.commons.rdf.api.RDF;
import org.apache.commons.rdf.jena.JenaRDF;
import org.apache.kafka.streams.KeyValue;
import org.junit.Before;
import org.junit.Test;

/**
 * @author acoburn
 */
public class StreamProcessingTest {

    private static final RDF rdf = new JenaRDF();

    private final String identifier1 = "trellis:repository/resource1";
    private final String identifier2 = "trellis:repository/resource1/resource2";

    private final Map<String, String> config = new HashMap<>();

    @Before
    public void setUp() throws Exception {
        config.put("repository", getClass().getResource("/root").toURI().toString());
    }

    @Test
    public void testAddContainment() {
        final IRI id1 = rdf.createIRI(identifier1);
        final IRI id2 = rdf.createIRI(identifier2);
        final Quad quad1 = rdf.createQuad(LDP.PreferContainment, id1, LDP.contains, id2);
        final Quad quad2 = rdf.createQuad(Trellis.PreferUserManaged, id2, DC.title, rdf.createLiteral("Title"));
        final Quad quad3 = rdf.createQuad(Trellis.PreferServerManaged, id2, type, LDP.RDFSource);

        final Dataset dataset = rdf.createDataset();

        dataset.add(quad1);
        dataset.add(quad2);
        dataset.add(quad3);

        final KeyValue<String, Dataset> res = StreamProcessing.addContainmentQuads(config, identifier1, dataset);
        assertEquals(identifier1, res.key);
        assertTrue(res.value.contains(quad1));
        assertTrue(res.value.contains(quad2));
        assertTrue(res.value.contains(quad3));
        assertEquals(3L, res.value.size());
    }

    @Test
    public void testDeleteContainment() {
        final IRI id1 = rdf.createIRI(identifier1);
        final IRI id2 = rdf.createIRI(identifier2);
        final Quad quad1 = rdf.createQuad(LDP.PreferContainment, id1, LDP.contains, id2);
        final Quad quad2 = rdf.createQuad(Trellis.PreferUserManaged, id2, DC.title, rdf.createLiteral("Title"));
        final Quad quad3 = rdf.createQuad(Trellis.PreferServerManaged, id2, type, LDP.RDFSource);

        final Dataset dataset = rdf.createDataset();

        dataset.add(quad1);
        dataset.add(quad2);
        dataset.add(quad3);

        final KeyValue<String, Dataset> res = StreamProcessing.deleteContainmentQuads(config, identifier1, dataset);
        assertEquals(identifier1, res.key);
        assertTrue(res.value.contains(quad1));
        assertTrue(res.value.contains(quad2));
        assertTrue(res.value.contains(quad3));
        assertEquals(3L, res.value.size());
    }
}
