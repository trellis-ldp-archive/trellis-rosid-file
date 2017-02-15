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
import static java.util.Objects.nonNull;
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
import java.util.Optional;
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
import org.apache.commons.rdf.api.Literal;
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
        rd.created = getFirstAsInstant(data.get(DC.created));
        rd.modified = getFirstAsInstant(data.get(DC.modified));

        // Populate datastream, if present
        graph.stream(identifier, DC.hasPart, null).map(Triple::getObject).findFirst().filter(id -> id instanceof IRI)
            .map(id -> (IRI) id).ifPresent(id -> {
                final Map<IRI, List<RDFTerm>> dsdata = graph.stream(id, null, null)
                    .collect(groupingBy(Triple::getPredicate, mapping(Triple::getObject, toList())));
                final Instant created = getFirstAsInstant(dsdata.get(DC.created));
                final Instant modified = getFirstAsInstant(dsdata.get(DC.modified));

                if (nonNull(created) && nonNull(modified)) {
                    rd.datastream = new ResourceData.DatastreamData();
                    rd.datastream.id = id.getIRIString();
                    rd.datastream.created = created;
                    rd.datastream.modified = modified;
                    rd.datastream.format = getFirstAsString(dsdata.get(DC.format));
                    rd.datastream.size = Long.parseLong(getFirstAsString(dsdata.get(DC.extent)));
                }
            });

        return rd;
    }

    @Override
    public Boolean isMemento() {
        return true;
    }

    @Override
    public Stream<MementoLink> getMementos() {
        return RDFPatch.asTimeMap(new File(directory, USER_JOURNAL)).map(range ->
                new MementoLinkImpl(rdf.createIRI(identifier.getIRIString() + "?version=" +
                        Long.toString(range.getUntil().getEpochSecond())), range.getFrom(), range.getUntil()));
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
        return Optional.of(new File(directory, CONTAINMENT_JOURNAL)).filter(File::exists)
            .map(file -> RDFPatch.asStream(rdf, file, time)).orElse(empty());
    }

    @Override
    protected Stream<Triple> getMembershipTriples() {
        return Optional.of(new File(directory, MEMBERSHIP_JOURNAL)).filter(File::exists)
            .map(file -> RDFPatch.asStream(rdf, file, time)).orElse(empty());
    }

    @Override
    protected Stream<Triple> getInboundTriples() {
        return Optional.of(new File(directory, INBOUND_JOURNAL)).filter(File::exists)
            .map(file -> RDFPatch.asStream(rdf, file, time)).orElse(empty());
    }

    @Override
    protected Stream<Triple> getUserTriples() {
        return Optional.of(new File(directory, USER_JOURNAL)).filter(File::exists)
            .map(file -> RDFPatch.asStream(rdf, file, time)).orElse(empty());
    }

    @Override
    protected Stream<Triple> getAuditTriples() {
        return Optional.of(new File(directory, AUDIT_JOURNAL)).filter(File::exists)
            .map(file -> RDFPatch.asStream(rdf, file, time)).orElse(empty());
    }


    private static String getFirstAsString(final List<RDFTerm> list) {
        return getStringStream(list).findFirst().orElse(null);
    }

    private static Instant getFirstAsInstant(final List<RDFTerm> list) {
        return ofNullable(list).orElse(emptyList()).stream().findFirst().flatMap(literalToInstant).orElse(null);
    }

    private static Stream<String> getStringStream(final List<RDFTerm> list) {
        return ofNullable(list).orElse(emptyList()).stream().flatMap(uriTermToString);
    }

    private static Function<RDFTerm, Stream<String>> uriTermToString = term -> {
        if (term instanceof IRI) {
            return of((IRI) term).map(IRI::getIRIString);
        }
        return empty();
    };

    private static Function<RDFTerm, Optional<Instant>> literalToInstant = term -> {
        if (term instanceof Literal) {
            return ofNullable((Literal) term).map(Literal::getLexicalForm).map(Instant::parse);
        }
        return Optional.empty();
    };

    private static ResourceData read(final File directory, final IRI identifier) {
        return read(directory, identifier, now());
    }
}
