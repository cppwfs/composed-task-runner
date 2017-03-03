/*
 *
 * Copyright 2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.task.app.composedtaskrunner;

import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.cloud.dataflow.core.dsl.ComposedTaskParser;
import org.springframework.cloud.dataflow.core.dsl.ComposedTaskVisitor;
import org.springframework.cloud.dataflow.core.dsl.TaskApp;
import org.springframework.cloud.dataflow.core.dsl.Transition;
import org.springframework.cloud.task.app.composedtaskrunner.properties.ComposedTaskProperties;
import org.springframework.cloud.task.app.composedtaskrunner.properties.PropertyUtility;
import org.springframework.context.EnvironmentAware;
import org.springframework.context.annotation.ImportBeanDefinitionRegistrar;
import org.springframework.core.env.Environment;
import org.springframework.core.type.AnnotationMetadata;

/**
 * Creates the the Steps necessary to execute the directed graph of a Composed
 * Task.
 *
 * @author Michael Minella
 * @author Glenn Renfro
 */
public class StepBeanDefinitionRegistrar implements ImportBeanDefinitionRegistrar, EnvironmentAware {

	private Environment env;

	@Override
	public void registerBeanDefinitions(AnnotationMetadata importingClassMetadata, BeanDefinitionRegistry registry) {
		ComposedTaskParser taskParser = new ComposedTaskParser();
		ComposedTaskProperties properties = composedTaskProperties();
		Map<String, Integer> taskSuffixMap = getTaskApps(taskParser, properties.getGraph());
		for(String taskName : taskSuffixMap.keySet()) {
			//handles the possibility that multiple instances of  task definition exist in a composed task
			for(int taskSuffix = 0; taskSuffixMap.get(taskName)>= taskSuffix ; taskSuffix++) {
				BeanDefinitionBuilder builder = BeanDefinitionBuilder.rootBeanDefinition(ComposedTaskRunnerStepFactory.class);
				builder.addConstructorArgValue(properties);
				builder.addConstructorArgValue(String.format("%s_%s", taskName, taskSuffix));
				builder.addConstructorArgValue(
						PropertyUtility.getPropertiesForTask(taskName,
								properties));
				builder.addConstructorArgValue(null);

				registry.registerBeanDefinition(String.format("%s_%s", taskName, taskSuffix), builder.getBeanDefinition());
			}
		}
	}

	@Override
	public void setEnvironment(Environment environment) {
		this.env = environment;
	}


	private ComposedTaskProperties composedTaskProperties() {
		ComposedTaskProperties properties = new ComposedTaskProperties();
		String dataFlowUri = this.env.getProperty("dataFlowUri");
		String maxWaitTime = this.env.getProperty("maxWaitTime");
		String intervalTimeBetweenChecks =
				this.env.getProperty("intervalTimeBetweenChecks");
		String jobNamePrefix = this.env.getProperty("jobNamePrefix");
		properties.setGraph(this.env.getProperty("graph"));
		properties.setComposedTaskArguments(
				this.env.getProperty("composedTaskArguments"));
		properties.setComposedTaskProperties("composedTaskProperties");

		if (maxWaitTime != null) {
			properties.setMaxWaitTime(Integer.valueOf(maxWaitTime));
		}
		if (intervalTimeBetweenChecks != null) {
			properties.setIntervalTimeBetweenChecks(Integer.valueOf(
					intervalTimeBetweenChecks));
		}
		if (dataFlowUri != null) {
			properties.setDataFlowUri(dataFlowUri);
		}
		if (jobNamePrefix != null) {
			properties.setJobNamePrefix(jobNamePrefix);
		}
		return properties;
	}

	/**
	 * @return all the apps referenced in the composed task.
	 */
	public Map<String, Integer> getTaskApps(ComposedTaskParser taskParser, String graph) {
		TaskAppsMapCollector collector = new TaskAppsMapCollector();
		taskParser.parse(graph).accept(collector);
		Map<String, Integer> taskApps = collector.getTaskApps();
		return taskApps;
	}

	/**
	 * Simple visitor that discovers all the tasks in use in the composed task definition.
	 */
	static class TaskAppsMapCollector extends ComposedTaskVisitor {

		Map<String, Integer> taskApps = new HashMap<>();

		@Override
		public void visit(TaskApp taskApp) {
			if (taskApps.containsKey(taskApp.getName())) {
				Integer updatedCount = taskApps.get(taskApp.getName()) + 1;
				taskApps.put(taskApp.getName(), updatedCount);
			}
			else {
				taskApps.put(taskApp.getName(), 0);
			}
		}

		@Override
		public void visit(Transition transition) {
			if (transition.isTargetApp()) {
				if (taskApps.containsKey(transition.getTargetApp())) {
					Integer updatedCount = taskApps.get(transition.getTargetApp()) + 1;
					taskApps.put(transition.getTargetApp(), updatedCount);
				}
				else {
					taskApps.put(transition.getTargetApp(), 0);
				}
			}
		}

		public Map<String, Integer> getTaskApps() {
			return taskApps;
		}

	}

}