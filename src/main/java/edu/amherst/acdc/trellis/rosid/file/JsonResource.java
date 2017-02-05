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

import java.time.Instant;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * @author acoburn
 */
class JsonResource {

    static class JsonDatastream {
        @JsonProperty("@id")
        public String id;

        @JsonProperty("format")
        public String format;

        @JsonProperty("size")
        public Long size;

        @JsonProperty("created")
        public Instant created;

        @JsonProperty("modified")
        public Instant modified;
    }

    @JsonProperty("@context")
    public String context = "http://acdc.amherst.edu/ns/trellisresource";

    @JsonProperty("@id")
    public String id;

    @JsonProperty("@type")
    public String ldpType;

    @JsonProperty("containedBy")
    public String containedBy;

    @JsonProperty("datastream")
    public JsonDatastream datastream;

    @JsonProperty("type")
    public List<String> userTypes;

    @JsonProperty("inbox")
    public String inbox;

    @JsonProperty("accessControl")
    public String accessControl;

    @JsonProperty("created")
    public Instant created;

    @JsonProperty("modified")
    public Instant modified;

    @JsonProperty("membershipResource")
    public String membershipResource;

    @JsonProperty("hasMemberRelation")
    public String hasMemberRelation;

    @JsonProperty("isMemberOfRelation")
    public String isMemberOfRelation;

    @JsonProperty("insertedContentRelation")
    public String insertedContentRelation;

    @JsonProperty("creator")
    public String creator;
}
