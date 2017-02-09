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

/**
 * @author acoburn
 */
class Constants {
    public static final String PREFIX = "info:trellis/";

    public static final String AUDIT_CACHE = "audit.cache";

    public static final String AUDIT_JOURNAL = "audit.journal";

    public static final String RESOURCE_CACHE = "resource.json";

    public static final String RESOURCE_JOURNAL = "resource.journal";

    public static final String CONTAINMENT_CACHE = "containment.cache";

    public static final String CONTAINMENT_JOURNAL = "containment.journal";

    public static final String MEMBERSHIP_CACHE = "membership.cache";

    public static final String MEMBERSHIP_JOURNAL = "membership.journal";

    public static final String INBOUND_CACHE = "inbound.cache";

    public static final String INBOUND_JOURNAL = "inbound.journal";

    public static final String MEMENTO_CACHE = "memento.cache";

    public static final String MEMENTO_JOURNAL = "memento.journal";

    public static final String USER_CACHE = "user.cache";

    public static final String USER_JOURNAL = "user.journal";

    private Constants() {
        // prevent instantiation
    }
}
