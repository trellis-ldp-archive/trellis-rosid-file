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

import static java.util.Objects.nonNull;
import static java.util.Objects.requireNonNull;
import static java.util.Optional.of;
import static java.util.stream.Collectors.toList;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import com.fasterxml.jackson.annotation.JsonProperty;
import edu.amherst.acdc.trellis.vocabulary.ACL;
import edu.amherst.acdc.trellis.vocabulary.DC;
import edu.amherst.acdc.trellis.vocabulary.LDP;
import edu.amherst.acdc.trellis.vocabulary.OA;
import edu.amherst.acdc.trellis.vocabulary.RDF;
import edu.amherst.acdc.trellis.vocabulary.Trellis;
import org.apache.commons.rdf.api.Dataset;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Literal;
import org.apache.commons.rdf.api.Triple;

/**
 * @author acoburn
 */
class ResourceData {

    static class DatastreamData {
        @JsonProperty("@id")
        public String id;

        public String format;

        public Long size;

        public Instant created;

        public Instant modified;
    }

    @JsonProperty("@context")
    public String context = "http://acdc.amherst.edu/ns/trellisresource";

    @JsonProperty("@id")
    public String id;

    @JsonProperty("@type")
    public String ldpType;

    @JsonProperty("type")
    public List<String> userTypes;

    public String containedBy;

    public DatastreamData datastream;

    public String inbox;

    public String annotationService;

    public String accessControl;

    public Instant created;

    public Instant modified;

    public String membershipResource;

    public String hasMemberRelation;

    public String isMemberOfRelation;

    public String insertedContentRelation;

    public String creator;

    private static final Function<Triple, String> objectUriAsString = triple ->
        ((IRI) triple.getObject()).getIRIString();

    private static final Function<Triple, String> objectLiteralAsString = triple ->
        ((Literal) triple.getObject()).getLexicalForm();

    public static Optional<ResourceData> from(final IRI identifier, final Dataset dataset) {
        requireNonNull(identifier, "identifier may not be null!");
        requireNonNull(dataset, "dataset may not be null!");

        final ResourceData rd = new ResourceData();
        rd.id = identifier.getIRIString();

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
                rd.datastream = new ResourceData.DatastreamData();
                rd.datastream.id = id.getIRIString();

                graph.stream(id, DC.created, null).findFirst().map(objectLiteralAsString).map(Instant::parse)
                    .ifPresent(date -> rd.datastream.created = date);

                graph.stream(id, DC.modified, null).findFirst().map(objectLiteralAsString).map(Instant::parse)
                    .ifPresent(date -> rd.datastream.modified = date);

                graph.stream(id, DC.format, null).findFirst().map(objectLiteralAsString)
                    .ifPresent(format -> rd.datastream.format = format);

                graph.stream(id, DC.extent, null).findFirst().map(objectLiteralAsString).map(Long::parseLong)
                    .ifPresent(size -> rd.datastream.size = size);
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

            graph.stream(identifier, OA.annotationService, null).findFirst().map(objectUriAsString)
                .ifPresent(res -> rd.annotationService = res);
        });
        return of(rd).filter(x -> nonNull(x.ldpType)).filter(x -> nonNull(x.created));
    }
}
