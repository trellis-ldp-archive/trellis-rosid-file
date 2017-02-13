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

import static java.time.Instant.now;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.partitioningBy;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Stream.empty;
import static java.util.stream.Stream.of;
import static edu.amherst.acdc.trellis.rosid.Constants.AUDIT_JOURNAL;
import static edu.amherst.acdc.trellis.rosid.Constants.CONTAINMENT_JOURNAL;
import static edu.amherst.acdc.trellis.rosid.Constants.INBOUND_JOURNAL;
import static edu.amherst.acdc.trellis.rosid.Constants.MEMBERSHIP_JOURNAL;
import static edu.amherst.acdc.trellis.rosid.Constants.RESOURCE_JOURNAL;
import static edu.amherst.acdc.trellis.rosid.Constants.USER_JOURNAL;

import java.io.File;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

import edu.amherst.acdc.trellis.api.MementoLink;
import edu.amherst.acdc.trellis.vocabulary.ACL;
import edu.amherst.acdc.trellis.vocabulary.DC;
import edu.amherst.acdc.trellis.vocabulary.LDP;
import edu.amherst.acdc.trellis.vocabulary.RDF;
import edu.amherst.acdc.trellis.vocabulary.Trellis;
import org.apache.commons.rdf.api.Graph;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.RDFTerm;
import org.apache.commons.rdf.api.Triple;

/**
 * An object that mediates access to the resource version files.
 *
 * @author acoburn
 */
class VersionedResource extends AbstractFileResource {

    private final Instant time;

    /**
     * Create a File-based versioned resource
     * @param directory the directory
     * @param identifier the resource identifier
     * @param time the time
     */
    public VersionedResource(final File directory, final IRI identifier, final Instant time) {
        super(directory, identifier);
        this.time = time;
        this.data = read(directory, identifier, time);
    }

    /**
     * Create a File-based versioned resource
     * @param directory the directory
     * @param identifier the resource identifier
     */
    public VersionedResource(final File directory, final IRI identifier) {
        this(directory, identifier, now());
    }

    public static ResourceData read(final File directory, final IRI identifier, final Instant time) {
        final Graph graph = rdf.createGraph();
        RDFPatch.asStream(rdf, new File(directory, RESOURCE_JOURNAL), time).forEach(graph::add);

        final Map<IRI, List<RDFTerm>> data = graph.stream(identifier, null, null)
            .collect(groupingBy(Triple::getPredicate, mapping(Triple::getObject, toList())));

        final Map<Boolean, List<String>> types = getStringStream(data.getOrDefault(RDF.type,
                    singletonList(LDP.Resource))).collect(partitioningBy(str -> str.startsWith(LDP.uri)));

        final ResourceData rd = new ResourceData();
        rd.id = identifier.getIRIString();
        rd.containedBy = getFirstAsString(data.get(Trellis.containedBy));
        rd.ldpType = types.get(true).get(0);
        rd.userTypes = types.get(false);
        rd.accessControl = getFirstAsString(data.get(ACL.accessControl));
        rd.inbox = getFirstAsString(data.get(LDP.inbox));
        rd.membershipResource = getFirstAsString(data.get(LDP.membershipResource));
        rd.hasMemberRelation = getFirstAsString(data.get(LDP.hasMemberRelation));
        rd.isMemberOfRelation = getFirstAsString(data.get(LDP.isMemberOfRelation));
        rd.insertedContentRelation = getFirstAsString(data.get(LDP.insertedContentRelation));
        rd.creator = getFirstAsString(data.get(DC.creator));
        // TODO -- populate rd with triple data
        //rd.created; // Instant
        //rd.modified; // Instant
        //rd.datastream;
        return rd;
    }

    private static String getFirstAsString(final List<RDFTerm> list) {
        return getStringStream(list).findFirst().orElse(null);
    }

    private static Stream<String> getStringStream(final List<RDFTerm> list) {
        return ofNullable(list).orElse(emptyList()).stream().flatMap(uriTermToString);
    }

    private static Function<RDFTerm, Stream<String>> uriTermToString = term -> {
        if (term instanceof IRI) {
            return Stream.of(((IRI) term).getIRIString());
        }
        return empty();
    };

    private static ResourceData read(final File directory, final IRI identifier) {
        return read(directory, identifier, now());
    }

    @Override
    public Boolean isMemento() {
        return true;
    }

    @Override
    public Stream<MementoLink> getMementos() {
        // TODO -- get from storage layer
        return Stream.empty();
    }

    @Override
    public Stream<IRI> getContains() {
        return getContainmentTriples().map(Triple::getSubject).flatMap(subj -> {
            if (subj instanceof IRI) {
                return of((IRI) subj);
            }
            return empty();
        });
    }

    @Override
    protected Stream<Triple> getContainmentTriples() {
        return RDFPatch.asStream(rdf, new File(directory, CONTAINMENT_JOURNAL), time);
    }


    protected Stream<Triple> getMembershipTriples() {
        return RDFPatch.asStream(rdf, new File(directory, MEMBERSHIP_JOURNAL), time);
    }

    protected Stream<Triple> getInboundTriples() {
        return RDFPatch.asStream(rdf, new File(directory, INBOUND_JOURNAL), time);
    }

    protected Stream<Triple> getUserTriples() {
        return RDFPatch.asStream(rdf, new File(directory, USER_JOURNAL), time);
    }

    protected Stream<Triple> getAuditTriples() {
        return RDFPatch.asStream(rdf, new File(directory, AUDIT_JOURNAL), time);
    }
}
