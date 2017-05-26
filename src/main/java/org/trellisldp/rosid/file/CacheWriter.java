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

import static java.util.Objects.isNull;
import static org.slf4j.LoggerFactory.getLogger;
import static org.trellisldp.rosid.file.Constants.RESOURCE_JOURNAL;
import static org.trellisldp.rosid.file.FileUtils.resourceDirectory;

import java.io.File;
import java.util.Map;

import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;

/**
 * @author acoburn
 */
public class CacheWriter extends DoFn<KafkaRecord<String, String>, KafkaRecord<String, String>> {

    private static final Logger LOGGER = getLogger(CacheWriter.class);

    private final Map<String, String> config;

    /**
     * Create a new cache writer processor
     * @param config the configuration
     */
    public CacheWriter(final Map<String, String> config) {
        super();
        this.config = config;
    }

    /**
     * A method for processing each element
     * @param c the context
     */
    @ProcessElement
    public void processElement(final ProcessContext c) {
        final String key = c.element().getKV().getKey();
        final File dir = resourceDirectory(config, key);
        if (!isNull(dir)) {
            final File file = new File(dir, RESOURCE_JOURNAL);
            if (!CachedResource.write(file, key)) {
                LOGGER.error("Error writing cached resource for {}", key);
            }
        } else {
            LOGGER.error("Error accessing cached resource location for {}", key);
        }
    }
}
