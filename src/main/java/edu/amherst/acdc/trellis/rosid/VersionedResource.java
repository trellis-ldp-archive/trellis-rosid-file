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

import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableSet;
import static java.util.Objects.nonNull;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Stream.empty;
import static edu.amherst.acdc.trellis.rosid.Constants.RESOURCE_JOURNAL;

import java.io.File;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import edu.amherst.acdc.trellis.api.Resource;
import edu.amherst.acdc.trellis.vocabulary.ACL;
import edu.amherst.acdc.trellis.vocabulary.DC;
import edu.amherst.acdc.trellis.vocabulary.LDP;
import edu.amherst.acdc.trellis.vocabulary.RDF;
import edu.amherst.acdc.trellis.vocabulary.Trellis;
import org.apache.commons.rdf.api.Graph;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Literal;
import org.apache.commons.rdf.api.Quad;
import org.apache.commons.rdf.api.RDFTerm;
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

    /**
     * Read the state of the resource data at a particular point in time
     * @param directory the directory
     * @param identifier the identifier
     * @param time the time
     * @return the resource data, if it exists
     */
    public static Optional<ResourceData> read(final File directory, final IRI identifier, final Instant time) {
        final Graph graph = rdf.createGraph();
        final File file = new File(directory, RESOURCE_JOURNAL);

        if (file.exists()) {

            final Map<IRI, List<IRI>> data = new HashMap<>();
            final ResourceData rd = new ResourceData();
            rd.id = identifier.getIRIString();

            RDFPatch.asStream(rdf, file, identifier, time).filter(quad -> quad.getGraphName().isPresent() &&
                    namedGraphs.contains(quad.getGraphName().get())).forEach(quad -> {
                if (quad.getGraphName().get().equals(Trellis.UserManagedTriples) &&
                        specialUserProperties.contains(quad.getPredicate()) && quad.getObject() instanceof IRI) {
                    data.computeIfAbsent(quad.getPredicate(), k -> new ArrayList<>()).add((IRI) quad.getObject());
                } else if (quad.getGraphName().get().equals(Trellis.ServerManagedTriples)) {
                    if (quad.getPredicate().equals(DC.created)) {
                        rd.created = Optional.of((Literal) quad.getObject()).map(Literal::getLexicalForm)
                            .map(Instant::parse).orElse(null);
                    } else if (quad.getPredicate().equals(DC.modified)) {
                        rd.modified = Optional.of((Literal) quad.getObject()).map(Literal::getLexicalForm)
                            .map(Instant::parse).orElse(null);
                    } else if (quad.getObject() instanceof IRI) {
                        data.computeIfAbsent(quad.getPredicate(), k -> new ArrayList<>()).add((IRI) quad.getObject());
                    }
                    graph.add(quad.asTriple());
                }
            });


            rd.ldpType = data.getOrDefault(RDF.type, emptyList()).stream().map(IRI::getIRIString)
                .filter(uri -> uri.startsWith(LDP.uri)).findFirst().orElse(null);
            rd.userTypes = data.getOrDefault(RDF.type, emptyList()).stream().map(IRI::getIRIString)
                .filter(uri -> !uri.startsWith(LDP.uri)).collect(toList());
            rd.containedBy = getFirstAsString(data.get(Trellis.containedBy));
            rd.accessControl = getFirstAsString(data.get(ACL.accessControl));
            rd.inbox = getFirstAsString(data.get(LDP.inbox));
            rd.membershipResource = getFirstAsString(data.get(LDP.membershipResource));
            rd.hasMemberRelation = getFirstAsString(data.get(LDP.hasMemberRelation));
            rd.isMemberOfRelation = getFirstAsString(data.get(LDP.isMemberOfRelation));
            rd.insertedContentRelation = getFirstAsString(data.get(LDP.insertedContentRelation));
            rd.creator = getFirstAsString(data.get(DC.creator));

            // Populate datastream, if present
            graph.stream(identifier, DC.hasPart, null).map(Triple::getObject).findFirst()
                .filter(id -> id instanceof IRI).map(id -> (IRI) id).ifPresent(id -> {
                    final Map<IRI, List<RDFTerm>> dsdata = graph.stream(id, null, null)
                        .collect(groupingBy(Triple::getPredicate, mapping(Triple::getObject, toList())));
                    final Instant created = ofNullable(dsdata.get(DC.created)).map(l -> l.get(0))
                        .map(term -> (Literal) term).map(Literal::getLexicalForm).map(Instant::parse).orElse(null);
                    final Instant modified = ofNullable(dsdata.get(DC.modified)).map(l -> l.get(0))
                        .map(term -> (Literal) term).map(Literal::getLexicalForm).map(Instant::parse).orElse(null);

                    if (nonNull(created) && nonNull(modified)) {
                        rd.datastream = new ResourceData.DatastreamData();
                        rd.datastream.id = id.getIRIString();
                        rd.datastream.created = created;
                        rd.datastream.modified = modified;
                        rd.datastream.format = ofNullable(dsdata.get(DC.format)).map(l -> l.get(0))
                            .map(term -> (Literal) term).map(Literal::getLexicalForm).orElse(null);
                        rd.datastream.size = ofNullable(dsdata.get(DC.extent)).map(l -> l.get(0))
                            .map(term -> (Literal) term).map(Literal::getLexicalForm).map(Long::parseLong).orElse(null);
                    }
                });

            if (nonNull(rd.ldpType) && nonNull(rd.created)) {
                return Optional.of(rd);
            }
        }
        return Optional.empty();
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

    private static String getFirstAsString(final List<IRI> list) {
        return ofNullable(list).filter(l -> !l.isEmpty()).map(l -> l.get(0)).map(IRI::getIRIString).orElse(null);
    }
}
