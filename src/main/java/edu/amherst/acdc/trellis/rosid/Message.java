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

import org.apache.commons.rdf.api.Graph;
import org.apache.commons.rdf.api.IRI;

/**
 * @author acoburn
 */
public class Message {

    private final IRI identifier;

    private final IRI model;

    private final Graph graph;

    /**
     * Create a Message object
     * @param identifier the identifier
     * @param model the model
     * @param graph the graph
     */
    public Message(final IRI identifier, final IRI model, final Graph graph) {
        this.identifier = identifier;
        this.model = model;
        this.graph = graph;
    }

    /**
     * Get the identifier
     * @return the identifier
     */
    public IRI getIdentifier() {
        return identifier;
    }

    /**
     * Get the model
     * @return the model
     */
    public IRI getModel() {
        return model;
    }

    /**
     * Get the graph
     * @return the graph
     */
    public Graph getGraph() {
        return graph;
    }
}
