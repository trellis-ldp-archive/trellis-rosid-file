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

import static org.junit.Assert.assertFalse;

import java.io.File;
import java.util.Optional;

import org.trellisldp.api.Resource;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.RDF;
import org.apache.commons.rdf.jena.JenaRDF;
import org.junit.Before;
import org.junit.Test;

/**
 * @author acoburn
 */
public class CachedResourceTest {

    private static final RDF rdf = new JenaRDF();

    private File file;
    private IRI identifier = rdf.createIRI("trellis:repository/resource");

    @Before
    public void setUp() throws Exception {
        file = new File(getClass().getResource("/res3").toURI());
    }

    @Test
    public void testNonExistent1() {
        final Optional<Resource> resource = CachedResource.find(null, identifier);
        assertFalse(resource.isPresent());
    }

    @Test
    public void testNonExistent2() {
        final Optional<Resource> resource = CachedResource.find(file, identifier);
        assertFalse(resource.isPresent());
    }
}
