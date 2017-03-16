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

import static edu.amherst.acdc.trellis.rosid.common.RDFUtils.getInstance;
import static edu.amherst.acdc.trellis.rosid.file.FileUtils.resourceDirectory;
import static edu.amherst.acdc.trellis.vocabulary.DC.created;
import static edu.amherst.acdc.trellis.vocabulary.PROV.wasGeneratedBy;
import static edu.amherst.acdc.trellis.vocabulary.Trellis.PreferAudit;
import static edu.amherst.acdc.trellis.vocabulary.Trellis.PreferServerManaged;
import static edu.amherst.acdc.trellis.vocabulary.Trellis.containedBy;
import static java.time.Instant.now;
import static java.util.Optional.of;
import static java.util.stream.Stream.empty;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.commons.rdf.api.Dataset;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Predicate;
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
            final Map<String, String> config) {
        return (identifier, dataset) -> {
            try {
                RDFPatch.write(resourceDirectory(config, identifier), empty(), dataset.stream(), now());
            } catch (final IOException ex) {
                LOGGER.error("Error adding LDP container triples to {}: {}", identifier, ex.getMessage());
            }
            return new KeyValue<>(identifier, dataset);
        };
    }

    /**
     * A mapping function for deleting LDP Container properties
     * @param config the storage configuration
     * @return a mapping function that generates an additional message for further processing
     */
    public static KeyValueMapper<String, Dataset, KeyValue<String, Dataset>> ldpDeleter(
            final Map<String, String> config) {
        return (identifier, dataset) -> {
            try {
                RDFPatch.write(resourceDirectory(config, identifier), dataset.stream(), empty(), now());
            } catch (final IOException ex) {
                LOGGER.error("Error removing LDP container triples from {}: {}", identifier, ex.getMessage());
            }
            return new KeyValue<>(identifier, dataset);
        };
    }

    /**
     * A mapping function for updating the resource cache
     * @param config the storage configuration
     * @return a terminating foreach action
     */
    public static KeyValueMapper<String, Dataset, KeyValue<String, Dataset>> cacheWriter(
            final Map<String, String> config) {
        return (identifier, dataset) -> {
            try {
                CachedResource.write(resourceDirectory(config, identifier), identifier);
            } catch (final IOException ex) {
                LOGGER.error("Error writing cache for {}: {}", identifier, ex.getMessage());
            }
            return new KeyValue<>(identifier, dataset);
        };
    }

    /**
     * A mapping function for updating resources
     * @param config the storage configuration
     * @return a mapping function that generates 0 or more messages for further processing
     */
    public static KeyValueMapper<String, Dataset, Iterable<KeyValue<String, Dataset>>> updater(
            final Map<String, String> config) {
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
            final Map<String, String> config) {
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

    /**
     * A predicate that always returns true
     */
    public static final Predicate<String, Dataset> otherwise = (k, v) -> true;

    /**
     * A predicate determining whether the dataset is new
     */
    public static final Predicate<String, Dataset> isNew = (identifier, dataset) ->
        dataset.contains(of(PreferServerManaged), getInstance().createIRI(identifier), created, null);

    /**
     * A predicate determining whether the given key is the parent of the original delete target
     */
    public static final Predicate<String, Dataset> isDeleteParent = (identifier, dataset) ->
        dataset.contains(of(PreferServerManaged), null, containedBy, getInstance().createIRI(identifier));

    /**
     * A predicate determining whether the given key is the original delete target
     */
    public static final Predicate<String, Dataset> isDeleteTarget = (identifier, dataset) ->
        dataset.contains(of(PreferAudit), getInstance().createIRI(identifier), wasGeneratedBy, null);

    private StreamProcessing() {
        // prevent instantiation
    }
}
