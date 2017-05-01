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

import static java.lang.Long.parseLong;
import static java.lang.System.getProperty;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.streams.kstream.TimeWindows.of;

import edu.amherst.acdc.trellis.rosid.common.Constants;
import edu.amherst.acdc.trellis.rosid.common.DatasetSerde;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.rdf.api.Dataset;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStreamBuilder;
//import org.apache.kafka.streams.state.Stores;

/**
 * @author acoburn
 */
final class StreamConfiguration {

    private static final Long WINDOW_SIZE = parseLong(getProperty("kafka.window.delay.ms", "5000"));

    // 2^12-1
    //private static final Integer CACHE_SIZE = parseInt(getProperty("kafka.window.cache.size", "4095"));

    private static final String CACHE_NAME = "trellis.caching";

    /**
     * Configure the KafkaStream processor
     * @param storage the storage configuration
     * @return the configured kafka stream processor
     */
    public static KafkaStreams configure(final Map<String, String> storage) {
        return configure(getProperty("kafka.bootstrap.servers"), storage);
    }

    /**
     * Configure the KafkaStream processor
     * @param bootstrapServers the kafka servers
     * @param storage the storage configuration
     * @return the configured kafka stream processor
     */
    public static KafkaStreams configure(final String bootstrapServers, final Map<String, String> storage) {
        final Map<String, Object> props = new HashMap<>();
        props.put(APPLICATION_ID_CONFIG, "trellis-repository-application");
        props.put(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        final Serde<String> kserde = new Serdes.StringSerde();
        final Serde<Dataset> vserde = new DatasetSerde();

        final KStreamBuilder builder = new KStreamBuilder();

        //if (!builder.globalStateStores().containsKey(CACHE_NAME)) {
            //builder.addStateStore(Stores.create(CACHE_NAME).withStringKeys().withStringValues()
                //.inMemory().maxEntries(CACHE_SIZE).build());
        //}

        builder.stream(kserde, vserde, Constants.TOPIC_INBOUND_ADD)
            .foreach((k, v) -> StreamProcessing.addInboundQuads(storage, k, v));

        builder.stream(kserde, vserde, Constants.TOPIC_INBOUND_DELETE)
            .foreach((k, v) -> StreamProcessing.deleteInboundQuads(storage, k, v));

        builder.stream(kserde, vserde, Constants.TOPIC_LDP_MEMBERSHIP_ADD)
            .map((k, v) -> StreamProcessing.addMembershipQuads(storage, k, v))
            .to(Constants.TOPIC_CACHE_AGGREGATE);

        builder.stream(kserde, vserde, Constants.TOPIC_LDP_MEMBERSHIP_DELETE)
            .map((k, v) -> StreamProcessing.deleteMembershipQuads(storage, k, v))
            .to(Constants.TOPIC_CACHE_AGGREGATE);

        builder.stream(kserde, vserde, Constants.TOPIC_LDP_CONTAINMENT_ADD)
            .map((k, v) -> StreamProcessing.addContainmentQuads(storage, k, v))
            .to(Constants.TOPIC_CACHE_AGGREGATE);

        builder.stream(kserde, vserde, Constants.TOPIC_LDP_CONTAINMENT_DELETE)
            .map((k, v) -> StreamProcessing.deleteContainmentQuads(storage, k, v))
            .to(Constants.TOPIC_CACHE_AGGREGATE);

        builder.stream(kserde, vserde, Constants.TOPIC_CACHE_AGGREGATE).groupByKey()
            .reduce((val1, val2) -> val1, of(WINDOW_SIZE), CACHE_NAME)
            .toStream((k, v) -> k.key())
            .to(Constants.TOPIC_CACHE);

        builder.stream(kserde, vserde, Constants.TOPIC_CACHE)
            .map((k, v) -> StreamProcessing.writeCacheQuads(storage, k, v))
            .to(Constants.TOPIC_EVENT);

        return new KafkaStreams(builder, new StreamsConfig(props));
    }


    private StreamConfiguration() {
        //prevent instantiation
    }
}
