/**
 * Copyright (C) 2006-2017 Talend Inc. - www.talend.com
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.talend.sdk.component.starter.server.service.build;

import lombok.Data;
import org.talend.sdk.component.starter.server.service.domain.Build;
import org.talend.sdk.component.starter.server.service.domain.Dependency;
import org.talend.sdk.component.starter.server.service.domain.ProjectRequest;
import org.talend.sdk.component.starter.server.service.event.GeneratorRegistration;
import org.talend.sdk.component.starter.server.service.facet.wadl.WADLFacet;
import org.talend.sdk.component.starter.server.service.template.TemplateRenderer;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static java.util.stream.Collectors.toList;
import static org.talend.sdk.component.starter.server.Versions.CXF;

@ApplicationScoped
public class GradleBuildGenerator implements BuildGenerator {

    @Inject
    private TemplateRenderer tpl;

    void register(@Observes final GeneratorRegistration init) {
        init.registerBuildType("Gradle", this);
    }

    @Override
    public Build createBuild(final ProjectRequest.BuildConfiguration buildConfiguration, final String packageBase,
            final Collection<Dependency> dependencies, final Collection<String> facets) {

        final Set<String> buildDependencies = new TreeSet<>();
        final List<String> configurations = new ArrayList<>();
        final Set<String> plugins = new TreeSet<>();
        final List<String> tasks = new ArrayList<>();
        final Set<String> imports = new TreeSet<>();
        final Set<String> javaMainSourceSets = new TreeSet<>();

        if (facets.contains(WADLFacet.Constants.NAME)) {
            buildDependencies.add("org.apache.cxf:cxf-tools-wadlto-jaxrs:" + CXF);
            imports.add("org.apache.cxf.tools.common.ToolContext");
            imports.add("org.apache.cxf.tools.wadlto.WADLToJava");

            tasks.add("def wadlGeneratedFolder = \"$buildDir/generated-sources/cxf\"\n" + "task generateWadlClient {\n"
                    + "  def wadl = \"$projectDir/src/main/resources/wadl/client.xml\"\n" + "\n"
                    + "  inputs.file(wadl)\n" + "  outputs.dir(wadlGeneratedFolder)\n" + "\n" + "  doLast {\n"
                    + "    new File(wadlGeneratedFolder).mkdirs()\n" + "\n" + "    new WADLToJava([\n"
                    + "      \"-d\", wadlGeneratedFolder,\n" + "      \"-p\", \"com.application.client.wadl\",\n"
                    + "      wadl\n" + "    ] as String[]).run(new ToolContext())\n" + "  }\n" + "}");
            javaMainSourceSets.add("srcDir wadlGeneratedFolder");
            javaMainSourceSets.add("project.tasks.compileJava.dependsOn project.tasks.generateWadlClient");
        }

        final GradleBuild model = new GradleBuild(buildConfiguration,
                dependencies
                        .stream()
                        .map(d -> "test".equals(d.getScope()) ? new Dependency(d, "testCompile") : d)
                        .map(d -> "runtime".equals(d.getScope()) ? new Dependency(d, "compile") : d)
                        .collect(toList()),
                buildDependencies, configurations, plugins, tasks, imports, javaMainSourceSets);
        return new Build(buildConfiguration.getArtifact(), buildConfiguration.getGroup(),
                buildConfiguration.getVersion(), "src/main/java", "src/test/java", "src/main/resources",
                "src/test/resources", "src/main/webapp", "build.gradle",
                tpl.render("generator/gradle/build.gradle", model), "build");
    }

    @Data
    public static class GradleBuild {

        private final ProjectRequest.BuildConfiguration build;

        private final Collection<Dependency> dependencies;

        private final Collection<String> buildDependencies;

        private final Collection<String> configurations;

        private final Collection<String> plugins;

        private final Collection<String> tasks;

        private final Collection<String> imports;

        private final Collection<String> javaMainSourceSets;
    }
}
