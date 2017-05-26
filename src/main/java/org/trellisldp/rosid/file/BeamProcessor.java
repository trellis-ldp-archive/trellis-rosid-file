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

import static java.time.Instant.now;
import static java.util.Objects.isNull;
import static java.util.Optional.of;
import static java.util.stream.Stream.empty;
import static org.apache.jena.riot.Lang.NQUADS;
import static org.apache.jena.riot.RDFDataMgr.read;
import static org.apache.jena.sparql.core.DatasetGraphFactory.create;
import static org.slf4j.LoggerFactory.getLogger;
import static org.trellisldp.rosid.file.Constants.RESOURCE_JOURNAL;
import static org.trellisldp.rosid.file.FileUtils.resourceDirectory;

import java.io.File;
import java.io.StringReader;
import java.util.Map;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.rdf.api.Dataset;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.jena.JenaRDF;
import org.apache.jena.sparql.core.DatasetGraph;
import org.slf4j.Logger;

/**
 * @author acoburn
 */
class BeamProcessor extends DoFn<KV<String, String>, KV<String, String>> {

    private static final JenaRDF rdf = new JenaRDF();

    private static final Logger LOGGER = getLogger(BeamProcessor.class);

    private static Dataset deserialize(final String data) {
        final DatasetGraph dataset = create();
        read(dataset, new StringReader(data), null, NQUADS);
        return rdf.asDataset(dataset);
    }

    private final Map<String, String> config;
    private final Boolean add;
    private final IRI graph;

    /**
     * A beam processor that handles raw NQUAD graphs
     * @param config the configuration
     * @param graph the relevant graph to use
     * @param add if true, quads will be added; otherwise they will be deleted
     */
    public BeamProcessor(final Map<String, String> config, final IRI graph, final Boolean add) {
        super();
        this.config = config;
        this.graph = graph;
        this.add = add;
    }

    /**
     * Process the element
     * @param c the context
     */
    @ProcessElement
    public void processElement(final ProcessContext c) {
        final KV<String, String> element = c.element();

        final File dir = resourceDirectory(config, element.getKey());
        if (!isNull(dir)) {
            final File file = new File(dir, RESOURCE_JOURNAL);
            final Dataset dataset = deserialize(element.getValue());
            if (RDFPatch.write(file,
                        add ? empty() : dataset.stream(of(graph), null, null, null),
                        add ? dataset.stream(of(graph), null, null, null) : empty(), now())) {
                c.output(c.element());
            } else {
                LOGGER.error("Error removing inbound ref quads from {}", element.getKey());
            }
        } else {
            LOGGER.error("Unable to write inbound quads to {}", element.getKey());
        }
    }
}
