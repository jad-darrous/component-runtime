/**
 * Copyright (C) 2006-2021 Talend Inc. - www.talend.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.talend.sdk.component.api.service.source;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Map;

import org.talend.sdk.component.api.record.Record;

/**
 * This service aims to retrieve a record iterator based on a configured dataset of a connector.
 * it's is expected that Producer has no extra-configuration on dataset and is a finite producer (not a queue for
 * example).
 */
public interface InputFinder extends Serializable {

    /**
     * Retrieve iterator.
     * 
     * @param pluginName : plugin id.
     * @param familyName : connector family name.
     * @param version : version of configuration.
     * @param configuration : dataset configuration.
     * @return
     */
    Iterator<Record> find(final String pluginName, //
            final String familyName, //
            final int version, //
            final Map<String, String> configuration);
}
