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

import static org.trellisldp.rosid.common.ResourceData.from;
import static org.trellisldp.rosid.file.Constants.RESOURCE_JOURNAL;
import static org.trellisldp.rosid.file.RDFPatch.asStream;
import static org.trellisldp.rosid.file.RDFPatch.asTimeMap;
import static java.util.Collections.unmodifiableSet;
import static java.util.Optional.of;
import static java.util.stream.Stream.empty;
import static org.slf4j.LoggerFactory.getLogger;

import org.trellisldp.api.Resource;
import org.trellisldp.api.VersionRange;
import org.trellisldp.rosid.common.ResourceData;
import org.trellisldp.vocabulary.ACL;
import org.trellisldp.vocabulary.LDP;
import org.trellisldp.vocabulary.OA;
import org.trellisldp.vocabulary.RDF;
import org.trellisldp.vocabulary.Trellis;

import java.io.File;
import java.time.Instant;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.apache.commons.rdf.api.Dataset;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.slf4j.Logger;

/**
 * An object that mediates access to the resource version files.
 *
 * @author acoburn
 */
class VersionedResource extends AbstractFileResource {

    private static final Logger LOGGER = getLogger(VersionedResource.class);

    /* User-controllable properties that become part of the core resource data */
    private static final Set<IRI> specialUserProperties = unmodifiableSet(new HashSet<IRI>() { {
        add(ACL.accessControl);
        add(LDP.inbox);
        add(LDP.membershipResource);
        add(LDP.hasMemberRelation);
        add(LDP.isMemberOfRelation);
        add(LDP.insertedContentRelation);
        add(OA.annotationService);
        add(RDF.type);
    }});

    private static final Predicate<Quad> isServerManagedTriple = quad ->
        quad.getGraphName().filter(Trellis.PreferServerManaged::equals).isPresent();

    private static final Predicate<Quad> isResourceTriple = isServerManagedTriple.or(quad ->
        quad.getGraphName().filter(Trellis.PreferUserManaged::equals).isPresent() &&
        specialUserProperties.contains(quad.getPredicate()));

    private final Instant time;

    /**
     * Find the resource at a particular point in time
     * @param directory the directory
     * @param identifier the identifier
     * @param time the time
     * @return the resource, if it exists at the given time
     */
    public static Optional<Resource> find(final File directory, final String identifier, final Instant time) {
        return find(directory, rdf.createIRI(identifier), time);
    }

    /**
     * Find the resource at a particular point in time
     * @param directory the directory
     * @param identifier the identifier
     * @param time the time
     * @return the resource, if it exists at the given time
     */
    public static Optional<Resource> find(final File directory, final IRI identifier, final Instant time) {
        return read(directory, identifier, time).map(data -> new VersionedResource(directory, identifier, data, time));
    }

    /**
     * Read the state of the resource data at a particular point in time
     * @param directory the directory
     * @param identifier the identifier
     * @param time the time
     * @return the resource data, if it exists
     */
    public static Optional<ResourceData> read(final File directory, final String identifier, final Instant time) {
        return read(directory, rdf.createIRI(identifier), time);
    }

    /**
     * Read the state of the resource data at a particular point in time
     * @param directory the directory
     * @param identifier the identifier
     * @param time the time
     * @return the resource data, if it exists
     */
    public static Optional<ResourceData> read(final File directory, final IRI identifier, final Instant time) {
        return of(new File(directory, RESOURCE_JOURNAL)).filter(File::exists).flatMap(file -> {
            final Dataset dataset = rdf.createDataset();
            asStream(rdf, file, identifier, time).filter(isResourceTriple).forEach(dataset::add);
            return from(identifier, dataset);
        });
    }

    /**
     * Create a File-based versioned resource
     * @param directory the directory
     * @param identifier the resource identifier
     * @param data the resource data
     * @param time the time
     */
    protected VersionedResource(final File directory, final IRI identifier, final ResourceData data,
            final Instant time) {
        super(directory, identifier, data);
        this.time = time;
        LOGGER.debug("Creating a Versioned Resource for {}", identifier.getIRIString());
    }

    @Override
    public Boolean isMemento() {
        return true;
    }

    @Override
    public Stream<VersionRange> getMementos() {
        return asTimeMap(new File(directory, RESOURCE_JOURNAL));
    }

    @Override
    public Stream<Quad> stream() {
        return of(new File(directory, RESOURCE_JOURNAL)).filter(File::exists)
            .map(file -> asStream(rdf, file, identifier, time)).orElse(empty());
    }
}
