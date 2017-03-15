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

import static edu.amherst.acdc.trellis.rosid.file.FileUtils.resourceDirectory;
import static java.time.Instant.now;
import static java.util.stream.Stream.empty;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.commons.rdf.api.Dataset;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Windowed;
import org.slf4j.Logger;

/**
 * @author acoburn
 */
final class StreamProcessing {

    private static final Logger LOGGER = getLogger(StreamProcessing.class);

    /**
     * A mapping function for updating LDP Container properties
     * @param config the storage configuration
     * @return a mapping function that generates an additional message for further processing
     */
    public static KeyValueMapper<String, Dataset, KeyValue<String, Dataset>> ldpAdder(
            final Map<String, Configuration.Storage> config) {
        return (identifier, dataset) -> {
            try {
                RDFPatch.write(resourceDirectory(config, identifier), empty(), dataset.stream(), now());
            } catch (final IOException ex) {
                LOGGER.error("Error adding LDP container triples to {}: {}", identifier, ex.getMessage());
            }
            return new KeyValue<>(identifier, null);
        };
    }

    /**
     * A mapping function for deleting LDP Container properties
     * @param config the storage configuration
     * @return a mapping function that generates an additional message for further processing
     */
    public static KeyValueMapper<String, Dataset, KeyValue<String, Dataset>> ldpDeleter(
            final Map<String, Configuration.Storage> config) {
        return (identifier, dataset) -> {
            try {
                RDFPatch.write(resourceDirectory(config, identifier), dataset.stream(), empty(), now());
            } catch (final IOException ex) {
                LOGGER.error("Error removing LDP container triples from {}: {}", identifier, ex.getMessage());
            }
            return new KeyValue<>(identifier, null);
        };
    }

    /**
     * A mapping function for updating the resource cache
     * @param config the storage configuration
     * @return a terminating foreach action
     */
    public static ForeachAction<Windowed<String>, Dataset> cacheWriter(
            final Map<String, Configuration.Storage> config) {
        return (window, data) -> {
            final String identifier = window.key();
            try {
                CachedResource.write(resourceDirectory(config, identifier), identifier);
            } catch (final IOException ex) {
                LOGGER.error("Error writing cache for {}: {}", identifier, ex.getMessage());
            }
        };
    }

    /**
     * A mapping function for updating resources
     * @param config the storage configuration
     * @return a mapping function that generates 0 or more messages for further processing
     */
    public static KeyValueMapper<String, Dataset, Iterable<KeyValue<String, Dataset>>> updater(
            final Map<String, Configuration.Storage> config) {
        // TODO
        // -- write dataset to resource
        // -- add prov:endedAtTime
        // -- re-cache parent, if this is new
        return (identifier, dataset) -> {
            //final File dir = resourceDirectory(config, identifier);
            final Stream<KeyValue<String, Dataset>> stream = empty();
            return new Iterable<KeyValue<String, Dataset>>() {
                @Override
                public Iterator<KeyValue<String, Dataset>> iterator() {
                    return stream.iterator();
                }
            };
        };
    }

    /**
     * A mapping function for deleting resources
     * @param config the storage configuration
     * @return a mapping function that generates 0 or more messages for further processing
     */
    public static KeyValueMapper<String, Dataset, Iterable<KeyValue<String, Dataset>>> deleter(
            final Map<String, Configuration.Storage> config) {
        // TODO
        // -- get child resources
        // -- get parent resources
        // -- delete resource
        // -- delete child resources
        // -- re-cache parent, if it exists
        return (identifier, dataset) -> {
            //final File dir = resourceDirectory(config, identifier);
            final Stream<KeyValue<String, Dataset>> stream = empty();
            return new Iterable<KeyValue<String, Dataset>>() {
                @Override
                public Iterator<KeyValue<String, Dataset>> iterator() {
                    return stream.iterator();
                }
            };
        };
    }

    private StreamProcessing() {
        // prevent instantiation
    }
}
