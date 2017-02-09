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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static java.time.Instant.parse;
import static java.util.stream.Collectors.toList;
import static java.util.stream.StreamSupport.stream;

import java.io.File;
import java.util.List;

import org.junit.Test;

/**
 * @author acoburn
 */
public class ReverseFileSpliteratorTest {

    @Test
    public void testReadBackwards() throws Exception {
        final File file = new File(getClass().getResource("/file.txt").toURI());
        final List<String> data = stream(new ReverseFileSpliterator(file), false).collect(toList());

        assertTrue(data.size() == 10);
        assertEquals("10", data.get(0));
        assertEquals("9", data.get(1));
        assertEquals("8", data.get(2));
        assertEquals("7", data.get(3));
        assertEquals("6", data.get(4));
        assertEquals("5", data.get(5));
        assertEquals("4", data.get(6));
        assertEquals("3", data.get(7));
        assertEquals("2", data.get(8));
        assertEquals("1", data.get(9));
    }

    @Test
    public void testReadBackwards2() throws Exception {

        final File file = new File(getClass().getResource("/file2.txt").toURI());
        final List<String> data = stream(new ReverseFileSpliterator(file, parse("2017-02-09T09:23:15Z")), false)
            .collect(toList());

        assertTrue(data.size() == 5);
        assertEquals("5", data.get(0));
        assertEquals("4", data.get(1));
        assertEquals("3", data.get(2));
        assertEquals("2", data.get(3));
        assertEquals("1", data.get(4));
    }
}
