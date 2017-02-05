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

import static java.time.Instant.parse;
import static com.fasterxml.jackson.databind.SerializationFeature.WRITE_DATES_AS_TIMESTAMPS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import edu.amherst.acdc.trellis.vocabulary.LDP;
import org.junit.Test;

/**
 * @author acoburn
 */
public class JsonResourceTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    static {
        MAPPER.configure(WRITE_DATES_AS_TIMESTAMPS, false);
        MAPPER.registerModule(new JavaTimeModule());
    }

    @Test
    public void testDeserialize() throws IOException {
        final JsonResource res = MAPPER.readValue(getClass().getResourceAsStream("/resource1.json"),
                JsonResource.class);

        assertEquals("info:trellis/resource1", res.id);
        assertEquals(LDP.Container.getIRIString(), res.ldpType);
        assertTrue(res.userTypes.contains("http://example.org/ns/CustomType"));
        assertEquals("info:trellis/", res.containedBy);
        assertEquals("http://receiver.example.org/inbox", res.inbox);
        assertEquals("info:trellis/acl/public", res.accessControl);
        assertEquals(parse("2017-02-05T09:31:12Z"), res.created);
        assertEquals("file:/path/to/datastream", res.datastream.id);
        assertEquals("image/jpeg", res.datastream.format);
        assertEquals(new Long(103527L), res.datastream.size);
        assertNull(res.insertedContentRelation);
    }
}
