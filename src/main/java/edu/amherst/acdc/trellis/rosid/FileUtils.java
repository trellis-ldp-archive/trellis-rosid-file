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

import static java.io.File.separator;
import static java.util.Objects.requireNonNull;
import static org.apache.commons.codec.digest.DigestUtils.md5Hex;

import java.io.File;
import java.util.zip.CRC32;

import org.apache.commons.rdf.api.IRI;

/**
 * @author acoburn
 */
class FileUtils {

    public static File getResourceDirectory(final File baseDir, final IRI identifier) {
        requireNonNull(baseDir, "baseDir must be nonnull!");
        requireNonNull(identifier, "identifier must not be null!");

        final CRC32 hasher = new CRC32();
        hasher.update(identifier.getIRIString().getBytes());
        final String intermediateDir = Long.toHexString(hasher.getValue());
        final String path = md5Hex(identifier.getIRIString());
        final StringBuilder builder = new StringBuilder();
        for (int i = 0; i < intermediateDir.length() / 2; ++i) {
            builder.append(intermediateDir.substring(i, i + 2));
            builder.append(separator);
        }
        final File resourceDir = new File(baseDir, builder.toString() + path);
        resourceDir.mkdirs();
        return resourceDir;
    }

    private FileUtils() {
        // prevent instantiation
    }
}
