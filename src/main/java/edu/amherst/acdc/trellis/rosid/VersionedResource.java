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
import static java.util.Objects.nonNull;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Stream.empty;
import static edu.amherst.acdc.trellis.rosid.Constants.RESOURCE_JOURNAL;

import java.io.File;
import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import edu.amherst.acdc.trellis.api.Resource;
import edu.amherst.acdc.trellis.vocabulary.ACL;
import edu.amherst.acdc.trellis.vocabulary.DC;
import edu.amherst.acdc.trellis.vocabulary.LDP;
import edu.amherst.acdc.trellis.vocabulary.RDF;
import edu.amherst.acdc.trellis.vocabulary.Trellis;
import org.apache.commons.rdf.api.Dataset;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Literal;
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

    private static final Set<IRI> namedGraphs = unmodifiableSet(new HashSet<IRI>() { {
        add(Trellis.ServerManagedTriples);
        add(Trellis.UserManagedTriples);
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

    private static Function<Triple, String> objectUriAsString = triple ->
        ((IRI) triple.getObject()).getIRIString();

    private static Function<Triple, String> objectLiteralAsString = triple ->
        ((Literal) triple.getObject()).getLexicalForm();

    /**
     * Read the state of the resource data at a particular point in time
     * @param directory the directory
     * @param identifier the identifier
     * @param time the time
     * @return the resource data, if it exists
     */
    public static Optional<ResourceData> read(final File directory, final IRI identifier, final Instant time) {
        final Dataset dataset = rdf.createDataset();
        final File file = new File(directory, RESOURCE_JOURNAL);
        final ResourceData rd = new ResourceData();

        if (file.exists()) {

            final Map<IRI, List<IRI>> data = new HashMap<>();
            rd.id = identifier.getIRIString();

            RDFPatch.asStream(rdf, file, identifier, time).filter(isServerManaged.or(isSpecialUserTriple))
                .forEach(dataset::add);

            dataset.getGraph(Trellis.ServerManagedTriples).ifPresent(graph -> {
                graph.stream(identifier, DC.created, null).findFirst().map(objectLiteralAsString).map(Instant::parse)
                    .ifPresent(date -> rd.created = date);

                graph.stream(identifier, DC.modified, null).findFirst().map(objectLiteralAsString).map(Instant::parse)
                    .ifPresent(date -> rd.modified = date);

                graph.stream(identifier, RDF.type, null).findFirst().map(objectUriAsString)
                    .ifPresent(type -> rd.ldpType = type);

                graph.stream(identifier, Trellis.containedBy, null).findFirst().map(objectUriAsString)
                    .ifPresent(res -> rd.containedBy = res);

                graph.stream(identifier, DC.creator, null).findFirst().map(objectUriAsString)
                    .ifPresent(agent -> rd.creator = agent);

                // Populate datastream, if present
                graph.stream(identifier, DC.hasPart, null).findFirst().map(Triple::getObject).map(x -> (IRI) x)
                        .ifPresent(id -> {
                    final ResourceData.DatastreamData ds = new ResourceData.DatastreamData();
                    ds.id = id.getIRIString();

                    graph.stream(id, DC.created, null).findFirst().map(objectLiteralAsString).map(Instant::parse)
                        .ifPresent(date -> ds.created = date);

                    graph.stream(id, DC.modified, null).findFirst().map(objectLiteralAsString).map(Instant::parse)
                        .ifPresent(date -> ds.modified = date);

                    graph.stream(id, DC.format, null).findFirst().map(objectLiteralAsString)
                        .ifPresent(format -> ds.format = format);

                    graph.stream(id, DC.extent, null).findFirst().map(objectLiteralAsString).map(Long::parseLong)
                        .ifPresent(size -> ds.size = size);

                    rd.datastream = ds;
                });
            });

            dataset.getGraph(Trellis.UserManagedTriples).ifPresent(graph -> {
                rd.userTypes = graph.stream(identifier, RDF.type, null).map(objectUriAsString).collect(toList());

                graph.stream(identifier, ACL.accessControl, null).findFirst().map(objectUriAsString)
                    .ifPresent(res -> rd.accessControl = res);

                graph.stream(identifier, LDP.inbox, null).findFirst().map(objectUriAsString)
                    .ifPresent(res -> rd.inbox = res);

                graph.stream(identifier, LDP.membershipResource, null).findFirst().map(objectUriAsString)
                    .ifPresent(res -> rd.membershipResource = res);

                graph.stream(identifier, LDP.hasMemberRelation, null).findFirst().map(objectUriAsString)
                    .ifPresent(res -> rd.hasMemberRelation = res);

                graph.stream(identifier, LDP.isMemberOfRelation, null).findFirst().map(objectUriAsString)
                    .ifPresent(res -> rd.isMemberOfRelation = res);

                graph.stream(identifier, LDP.insertedContentRelation, null).findFirst().map(objectUriAsString)
                    .ifPresent(res -> rd.insertedContentRelation = res);
            });
        }
        return Optional.of(rd).filter(x -> nonNull(x.ldpType)).filter(x -> nonNull(x.created));
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
