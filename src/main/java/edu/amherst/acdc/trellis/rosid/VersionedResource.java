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

import static java.util.Collections.unmodifiableSet;
import static java.util.stream.Stream.empty;
import static edu.amherst.acdc.trellis.rosid.Constants.RESOURCE_JOURNAL;

import java.io.File;
import java.time.Instant;
import java.util.Collection;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Stream;

import edu.amherst.acdc.trellis.api.Resource;
import edu.amherst.acdc.trellis.vocabulary.ACL;
import edu.amherst.acdc.trellis.vocabulary.LDP;
import edu.amherst.acdc.trellis.vocabulary.RDF;
import edu.amherst.acdc.trellis.vocabulary.Trellis;
import org.apache.commons.rdf.api.Dataset;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.apache.commons.rdf.api.Triple;

/**
 * An object that mediates access to the resource version files.
 *
 * @author acoburn
 */
class VersionedResource extends AbstractFileResource {

    private static final Set<IRI> specialUserProperties = unmodifiableSet(new HashSet<IRI>() { {
        add(ACL.accessControl);
        add(LDP.inbox);
        add(LDP.membershipResource);
        add(LDP.hasMemberRelation);
        add(LDP.isMemberOfRelation);
        add(LDP.insertedContentRelation);
        add(RDF.type);
    }});

    private final Instant time;

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

    private static Predicate<Quad> isServerManaged = quad ->
        quad.getGraphName().filter(Trellis.ServerManagedTriples::equals).isPresent();

    private static Predicate<Quad> isSpecialUserTriple = quad ->
        quad.getGraphName().filter(Trellis.UserManagedTriples::equals).isPresent() &&
        specialUserProperties.contains(quad.getPredicate());

    /**
     * Read the state of the resource data at a particular point in time
     * @param directory the directory
     * @param identifier the identifier
     * @param time the time
     * @return the resource data, if it exists
     */
    public static Optional<ResourceData> read(final File directory, final IRI identifier, final Instant time) {
        return Optional.of(new File(directory, RESOURCE_JOURNAL)).filter(File::exists).flatMap(file -> {
            final Dataset dataset = rdf.createDataset();
            RDFPatch.asStream(rdf, file, identifier, time).filter(isServerManaged.or(isSpecialUserTriple))
                .forEach(dataset::add);
            return ResourceData.from(identifier, dataset);
        });
    }

    @Override
    public Boolean isMemento() {
        return true;
    }

    @Override
    public <T extends Resource.TripleCategory> Stream<Triple> stream(final Collection<T> category) {
        return Optional.of(new File(directory, RESOURCE_JOURNAL)).filter(File::exists)
            .map(file -> RDFPatch.asStream(rdf, file, identifier, time)).orElse(empty())
            .filter(quad -> quad.getGraphName().isPresent() &&
                    category.contains(categorymap.get(quad.getGraphName().get()))).map(Quad::asTriple);
    }
}
