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
package org.talend.sdk.component.runtime.manager.service.api;

import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import org.talend.sdk.component.runtime.base.Lifecycle;
import org.talend.sdk.component.runtime.manager.ComponentFamilyMeta;
import org.talend.sdk.component.runtime.manager.ComponentManager.ComponentType;
import org.talend.sdk.component.runtime.manager.ContainerComponentRegistry;

import lombok.RequiredArgsConstructor;

@FunctionalInterface
public interface ComponentInstantiator {

    Lifecycle instantiate(final Map<String, String> configuration, final int configVersion);

    @FunctionalInterface
    interface Builder {

        ComponentInstantiator build(final String pluginId, final String name, final ComponentType componentType);
    }

    @RequiredArgsConstructor
    class BuilderDefault implements Builder {

        private final Supplier<ContainerComponentRegistry> registryGetter;

        @Override
        public ComponentInstantiator build(final String pluginIdentifier, final String name,
                final ComponentType componentType) {
            return Optional
                    .ofNullable(this.registryGetter.get())
                    .map((ContainerComponentRegistry registry) -> registry.findComponentFamily(pluginIdentifier))
                    .map(componentType::findMeta)
                    .map((map) -> map.get(name))
                    .map((ComponentFamilyMeta.BaseMeta c) -> (ComponentInstantiator) c::instantiate)
                    .orElse(null);
        }
    }
}
