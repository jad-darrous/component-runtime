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
package org.talend.sdk.component.junit.environment;

import static java.util.Optional.ofNullable;

import java.io.File;
import java.lang.annotation.Annotation;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.stream.Stream;

import org.jboss.shrinkwrap.resolver.api.maven.coordinate.MavenDependency;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class ClassLoaderEnvironment extends BaseEnvironmentProvider {

    protected abstract MavenDependency[] rootDependencies();

    @Override
    protected AutoCloseable doStart(final Class<?> clazz, final Annotation[] annotations) {
        try {
            ofNullable(Thread.currentThread().getContextClassLoader())
                    .orElseGet(ClassLoader::getSystemClassLoader)
                    .loadClass("org.jboss.shrinkwrap.resolver.api.maven.Maven");
        } catch (final ClassNotFoundException e) {
            throw new IllegalStateException("Don't forget to add to your dependencies:\n"
                    + "  org.jboss.shrinkwrap.resolver:shrinkwrap-resolver-impl-maven:3.1.4");
        }
        final Thread thread = Thread.currentThread();
        log.info("Resolve dependency {} in context {}", clazz.getName(), this.getClass().getName());
        final File compressFile = new File("/root/.m2/repository/org/apache/commons/commons-compress/1.20/commons-compress-1.20.jar");
        final File bomFile = new File("/root/.m2/repository/org/talend/sdk/component/component-bom/1.35.0-SNAPSHOT/component-bom-1.35.0-SNAPSHOT.pom");
        final File bomFolder = new File("/root/.m2/repository/org/talend/sdk/component/component-bom");
        log.info("COMPRESS JAR FILE exists={}", compressFile.exists());
        log.info("BOM FILE exists={}", bomFile.exists());
        log.info("BOM FOLDER exists={}", bomFolder.exists());
        final URLClassLoader classLoader = new URLClassLoader(Stream
                .of(rootDependencies())
                .peek(dep -> log.info("Resolving " + dep + "..."))
                .flatMap(dep -> Stream.of(Dependencies.resolve(dep)))
                .peek((URL url) -> log.info("Found dependencies {}", url.getPath()))
                .toArray(URL[]::new), Thread.currentThread().getContextClassLoader());
        final ClassLoader original = thread.getContextClassLoader();
        thread.setContextClassLoader(classLoader);
        return () -> {
            thread.setContextClassLoader(original);
            classLoader.close();
        };
    }
}
