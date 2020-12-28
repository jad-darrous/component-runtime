/**
 * Copyright (C) 2006-2020 Talend Inc. - www.talend.com
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
package org.talend.sdk.component.runtime.manager.spi;

import java.util.List;
import java.util.Map;

public interface MigrationHandlerListenerExtension {

    void onAddKey(final Map<String, String> data, final String key, final String value);

    void onRenameKey(final Map<String, String> data, final String oldKey, final String newKey);

    void onRemoveKey(final Map<String, String> data, final String key);

    void onChangeValue(final Map<String, String> data, final String key, final String oldValue, final String newValue);

    void onSplitProperty(final Map<String, String> data, final String oldKey, final List<String> newKeys);

    void onMergeProperties(final Map<String, String> data, final List<String> oldKeys, final String newKey);

}