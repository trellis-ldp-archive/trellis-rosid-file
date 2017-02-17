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
import static java.util.Collections.singletonList;
import static java.util.Objects.nonNull;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Stream.empty;
import static java.util.stream.Stream.of;
import static edu.amherst.acdc.trellis.api.Resource.TripleContext.FEDORA_INBOUND_REFERENCES;
import static edu.amherst.acdc.trellis.api.Resource.TripleContext.LDP_CONTAINMENT;
import static edu.amherst.acdc.trellis.api.Resource.TripleContext.LDP_MEMBERSHIP;
import static edu.amherst.acdc.trellis.api.Resource.TripleContext.TRELLIS_AUDIT;
import static edu.amherst.acdc.trellis.api.Resource.TripleContext.USER_MANAGED;
import static edu.amherst.acdc.trellis.rosid.Constants.RESOURCE_JOURNAL;

import java.io.File;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Optional;
import java.util.stream.Stream;

import edu.amherst.acdc.trellis.api.Resource;
import edu.amherst.acdc.trellis.vocabulary.ACL;
import edu.amherst.acdc.trellis.vocabulary.DC;
import edu.amherst.acdc.trellis.vocabulary.Fedora;
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

    private final Instant time;
    private final Map<IRI, Resource.TripleCategory> categorymap = new HashMap<>();

    /**
     * Create a File-based versioned resource
     * @param directory the directory
     * @param identifier the resource identifier
     * @param time the time
     */
    protected VersionedResource(final File directory, final IRI identifier, final ResourceData data,
            final Instant time) {
        super(directory, identifier, data);
        this.time = time;

        categorymap.put(Fedora.InboundReferences, FEDORA_INBOUND_REFERENCES);
        categorymap.put(LDP.PreferContainment, LDP_CONTAINMENT);
        categorymap.put(LDP.PreferMembership, LDP_MEMBERSHIP);
        categorymap.put(Trellis.PreferAudit, TRELLIS_AUDIT);
        categorymap.put(Trellis.UserManagedTriples, USER_MANAGED);
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
        final Set<IRI> specialUserProperties = new HashSet<>();
        specialUserProperties.add(ACL.accessControl);
        specialUserProperties.add(LDP.inbox);
        specialUserProperties.add(LDP.membershipResource);
        specialUserProperties.add(LDP.hasMemberRelation);
        specialUserProperties.add(LDP.isMemberOfRelation);
        specialUserProperties.add(LDP.insertedContentRelation);
        specialUserProperties.add(RDF.type);

        final Graph graph = rdf.createGraph();
        final File file = new File(directory, RESOURCE_JOURNAL);
        final Set<IRI> types = new HashSet<>();
        types.add(Trellis.ServerManagedTriples);
        types.add(Trellis.UserManagedTriples);

        if (file.exists()) {

            final Map<IRI, List<IRI>> data = new HashMap<>();
            final ResourceData rd = new ResourceData();
            rd.id = identifier.getIRIString();

            RDFPatch.asStream(rdf, file, time).filter(quad -> quad.getGraphName().isPresent() &&
                    types.contains(quad.getGraphName().get())).forEach(quad -> {
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
    public Stream<IRI> getContains() {
        return stream(singletonList(LDP_CONTAINMENT)).map(Triple::getSubject).flatMap(subj -> {
            if (subj instanceof IRI) {
                return of((IRI) subj);
            }
            return empty();
        });
    }

    @Override
    public <T extends Resource.TripleCategory> Stream<Triple> stream(final Collection<T> category) {
        return Optional.of(new File(directory, RESOURCE_JOURNAL)).filter(File::exists)
            .map(file -> RDFPatch.asStream(rdf, file, time)).orElse(empty())
            .filter(quad -> quad.getGraphName().filter(categorymap::containsKey).isPresent() &&
                    category.contains(categorymap.get(quad.getGraphName().get())))
            .map(Quad::asTriple);
    }

    private static String getFirstAsString(final List<IRI> list) {
        return ofNullable(list).filter(l -> !l.isEmpty()).map(l -> l.get(0)).map(IRI::getIRIString).orElse(null);
    }
}
