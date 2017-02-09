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
import static java.util.Spliterator.ORDERED;
import static java.util.Spliterator.IMMUTABLE;
import static java.util.Spliterator.NONNULL;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Spliterator;
import java.util.function.Consumer;

import org.apache.commons.io.input.ReversedLinesFileReader;

/**
 * @author acoburn
 */
class ReverseFileSpliterator implements Spliterator<String> {

    private final ReversedLinesFileReader reader;

    /**
     * Create a spliterator that reads a file line-by-line in reverse
     * @param file the file
     */
    ReverseFileSpliterator(final File file) {
        try {
            this.reader = new ReversedLinesFileReader(file, UTF_8);
        } catch (final IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    @Override
    public void forEachRemaining(final Consumer<? super String> action) {
        try {
            for (String line = reader.readLine(); line != null; line = reader.readLine()) {
                action.accept((String) line);
            }
        } catch (final IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    @Override
    public boolean tryAdvance(final Consumer<? super String> action) {
        try {
            final String line = reader.readLine();
            if (line != null) {
                action.accept((String) line);
                return true;
            }
            return false;
        } catch (final IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    @Override
    public Spliterator<String> trySplit() {
        return null;
    }

    @Override
    public long estimateSize() {
        return Long.MAX_VALUE;
    }

    @Override
    public int characteristics() {
        return ORDERED | NONNULL | IMMUTABLE;
    }
}

