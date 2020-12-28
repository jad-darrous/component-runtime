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
package org.talend.sdk.component.runtime.manager.component;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;
import java.util.function.Predicate;

import org.talend.sdk.component.api.component.MigrationHandler;
import org.talend.sdk.component.runtime.manager.spi.MigrationHandlerListenerExtension;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractMigrationHandler implements MigrationHandler {

    private final Collection<MigrationHandlerListenerExtension> listeners = new CopyOnWriteArrayList<>();

    @Getter
    protected Map<String, String> configuration;

    /**
     * @param incomingVersion the version of associatedData values.
     * @param incomingData the data sent from the caller. Keys are using the path of the property as in component
     * metadata.
     *
     * @return the set of properties for the current version.
     */
    @Override
    public final Map<String, String> migrate(final int incomingVersion, final Map<String, String> incomingData) {
        configuration = new HashMap<>(incomingData);
        migrate(incomingVersion);

        return Collections.unmodifiableMap(configuration);
    }

    /**
     * @param incomingVersion the version of associated data values.
     */
    public abstract void migrate(final int incomingVersion);

    /**
     * @param oldKey configuration key
     * @param newKeys new split keys
     */
    public abstract void doSplitProperty(final String oldKey, final List<String> newKeys);

    /**
     * @param oldKeys configuration keys
     * @param newKey merged key
     */
    public abstract void doMergeProperties(final List<String> oldKeys, final String newKey);

    /**
     * @param listener the observer that will receive changes
     */
    public final synchronized void registerListener(final MigrationHandlerListenerExtension listener) {
        log.debug("[registerListener] registering {}.", listener.getClass().getName());
        listeners.add(listener);
    }

    /**
     * @param listener the observer that will receive changes
     */
    public final synchronized void unRegisterListener(final MigrationHandlerListenerExtension listener) {
        log.debug("[unRegisterListener] unregistering {}.", listener.getClass().getName());
        listeners.remove(listener);
    }

    private void checkKeyExistance(final String key) throws MigrationException {
        if (!configuration.containsKey(key)) {
            throw new MigrationException(String.format("Key %s does not exist", key));
        }
    }

    /**
     * @param key configuration key
     * @param value value for configuration key
     *
     * @throws MigrationException If missing key
     */
    public final void addKey(final String key, final String value) throws MigrationException {
        if (configuration.containsKey(key)) {
            throw new MigrationException(String.format("Key %s already exists", key));
        }
        configuration.put(key, value);

        listeners.forEach(e -> e.onAddKey(configuration, key, value));
    }

    /**
     * @param oldKey configuration key existing
     * @param newKey configuration key to rename
     *
     * @throws MigrationException If missing key
     */
    public final void renameKey(final String oldKey, final String newKey) throws MigrationException {
        checkKeyExistance(oldKey);
        configuration.put(newKey, configuration.get(oldKey));
        configuration.remove(oldKey);

        listeners.forEach(e -> e.onRenameKey(configuration, oldKey, newKey));
    }

    /**
     * @param key configuration key
     *
     * @throws MigrationException If missing key
     */
    public final void removeKey(final String key) throws MigrationException {
        checkKeyExistance(key);
        configuration.remove(key);

        listeners.forEach(e -> e.onRemoveKey(configuration, key));
    }

    /**
     * @param key configuration key
     * @param newValue new value to set in migration
     *
     * @throws MigrationException If missing key
     */
    public final void changeValue(final String key, final String newValue) throws MigrationException {
        checkKeyExistance(key);
        final String oldValue = configuration.get(key);
        configuration.put(key, newValue);

        listeners.forEach(e -> e.onChangeValue(configuration, key, oldValue, newValue));
    }

    /**
     * @param key configuration key
     * @param newValue new value to set in migration
     * @param condition predicate that tests current value, if true sets new value
     *
     * @throws MigrationException If missing key
     */
    public final void changeValue(final String key, final String newValue, final Predicate<String> condition)
            throws MigrationException {
        checkKeyExistance(key);
        final String oldValue = configuration.get(key);
        if (condition.test(oldValue)) {
            changeValue(key, newValue);
        }
    }

    /**
     * @param key configuration key
     * @param updater function to set new value
     *
     * @throws MigrationException If missing key
     */
    public final void changeValue(final String key, final Function<? super String, String> updater)
            throws MigrationException {
        checkKeyExistance(key);
        final String oldValue = configuration.get(key);
        final String newValue = updater.apply(oldValue);
        changeValue(key, newValue);
    }

    /**
     * @param key configuration key
     * @param updater function to set new value
     * @param condition predicate that tests current value, if true sets new value
     *
     * @throws MigrationException If missing key
     */
    public final void changeValue(final String key, final Function<? super String, String> updater,
            final Predicate<String> condition) throws MigrationException {
        checkKeyExistance(key);
        final String oldValue = configuration.get(key);
        if (condition.test(oldValue)) {
            final String newValue = updater.apply(oldValue);
            changeValue(key, newValue);
        }
    }

    /**
     * @param oldKey configuration key
     * @param newKeys new split keys
     *
     * @throws MigrationException If missing key
     */
    public final void splitProperty(final String oldKey, final List<String> newKeys) throws MigrationException {
        checkKeyExistance(oldKey);
        doSplitProperty(oldKey, newKeys);

        listeners.forEach(e -> e.onSplitProperty(configuration, oldKey, newKeys));
    }

    /**
     * @param oldKeys configuration keys
     * @param newKey merged key
     *
     * @throws MigrationException If missing key
     */
    public final void mergeProperties(final List<String> oldKeys, final String newKey) throws MigrationException {
        for (String k : oldKeys) {
            checkKeyExistance(k);
        }
        doMergeProperties(oldKeys, newKey);

        listeners.forEach(e -> e.onMergeProperties(configuration, oldKeys, newKey));
    }

    public class MigrationException extends Exception {

        public MigrationException(final String message) {
            super(message);
        }
    }

}