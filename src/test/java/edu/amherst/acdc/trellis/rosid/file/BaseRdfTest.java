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

import edu.amherst.acdc.trellis.vocabulary.LDP;
import edu.amherst.acdc.trellis.vocabulary.Trellis;
import edu.amherst.acdc.trellis.vocabulary.Fedora;

import java.util.function.Predicate;

import org.apache.commons.rdf.api.RDF;
import org.apache.commons.rdf.api.Quad;
import org.apache.commons.rdf.jena.JenaRDF;

/**
 * @author acoburn
 */
class BaseRdfTest {

    protected static final RDF rdf = new JenaRDF();

    protected static final Predicate<Quad> isUserManaged = quad ->
        quad.getGraphName().filter(Trellis.PreferUserManaged::equals).isPresent();

    protected static final Predicate<Quad> isServerManaged = quad ->
        quad.getGraphName().filter(Trellis.PreferServerManaged::equals).isPresent();

    protected static final Predicate<Quad> isContainment = quad ->
        quad.getGraphName().filter(LDP.PreferContainment::equals).isPresent();

    protected static final Predicate<Quad> isMembership = quad ->
        quad.getGraphName().filter(LDP.PreferMembership::equals).isPresent();

    protected static final Predicate<Quad> isInbound = quad ->
        quad.getGraphName().filter(Fedora.PreferInboundReferences::equals).isPresent();

    protected BaseRdfTest() {

    }
}
