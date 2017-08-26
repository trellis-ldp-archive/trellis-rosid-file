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

import static java.time.Instant.now;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;
import static org.trellisldp.rosid.file.Constants.MEMENTO_CACHE;
import static org.trellisldp.rosid.file.Constants.RESOURCE_CACHE;

import java.io.File;
import java.util.NoSuchElementException;
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

    private File file5, file3, readonly, readonly2, ldprs;
    private IRI identifier = rdf.createIRI("trellis:repository/resource");

    @Before
    public void setUp() throws Exception {
        ldprs = new File(getClass().getResource("/ldprs").toURI());
        file5 = new File(getClass().getResource("/res5").toURI());
        file3 = new File(getClass().getResource("/res3").toURI());
        readonly = new File(getClass().getResource("/readonly").toURI());
        readonly2 = new File(getClass().getResource("/readonly2").toURI());
    }

    @Test
    public void testNonExistent1() {
        final Optional<Resource> resource = CachedResource.find(null, identifier);
        assertFalse(resource.isPresent());
    }

    @Test
    public void testNonExistent2() {
        final Optional<Resource> resource = CachedResource.find(file5, identifier);
        assertTrue(resource.isPresent());
        assertFalse(resource.get().stream().findFirst().isPresent());
    }

    @Test
    public void testNonExistent3() {
        final Optional<Resource> resource = CachedResource.find(file3, identifier);
        assertFalse(resource.isPresent());
    }

    @Test(expected = NoSuchElementException.class)
    public void testMementoReader() throws Exception {
        try (final CachedResource.MementoReader reader = new CachedResource.MementoReader(new File(ldprs,
                    MEMENTO_CACHE))) {
            while (reader.hasNext()) {
                assertNotNull(reader.next());
            }
            reader.next();
        }
    }

    @Test
    public void testWriteNonExistent() {
        final File fileUnknown = new File(file3, "testing");
        assertFalse(CachedResource.write(fileUnknown, identifier, now()));
    }

    @Test
    public void testWriteOk() {
        assumeTrue(readonly.setWritable(true));
        assertTrue(CachedResource.write(readonly, identifier, now()));
    }

    @Test
    public void testWriteError() {
        assumeTrue(readonly.setWritable(false));
        assertFalse(CachedResource.write(readonly, identifier, now()));
    }

    @Test
    public void testWriteErrorResource() {
        assumeTrue(readonly2.setWritable(true));
        assumeTrue(new File(readonly2, RESOURCE_CACHE).setWritable(false));
        assertFalse(CachedResource.write(readonly2, identifier, now()));
    }

    @Test
    public void testWriteError2() {
        assumeTrue(readonly2.setWritable(true));
        assumeTrue(new File(readonly2, MEMENTO_CACHE).setWritable(false));
        assertFalse(CachedResource.write(readonly2, identifier, now()));
    }
}
