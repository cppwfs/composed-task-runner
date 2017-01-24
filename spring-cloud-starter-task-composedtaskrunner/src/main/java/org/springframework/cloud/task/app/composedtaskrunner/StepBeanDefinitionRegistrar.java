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

import java.util.UUID;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.task.app.composedtaskrunner.properties.ComposedTaskProperties;
import org.springframework.cloud.task.app.composedtaskrunner.properties.PropertyUtility;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportBeanDefinitionRegistrar;
import org.springframework.core.type.AnnotationMetadata;

/**
 * Created by glennrenfro on 1/24/17.
 */
public class StepBeanDefinitionRegistrar implements ImportBeanDefinitionRegistrar {

	@Autowired
	private ComposedTaskProperties properties;

	@Override
	public void registerBeanDefinitions(AnnotationMetadata importingClassMetadata, BeanDefinitionRegistry registry) {

		BeanDefinitionBuilder builder = BeanDefinitionBuilder.rootBeanDefinition(ComposedTaskRunnerStepFactory.class);

		builder.addConstructorArgReference("taskRepository");
		builder.addConstructorArgReference("taskOperations");
		builder.addConstructorArgReference("taskExplorer");
		builder.addConstructorArgValue(this.properties);
		String taskName = this.properties.getGraph();
		builder.addConstructorArgValue(taskName);
		builder.addConstructorArgValue(
				PropertyUtility.getPropertiesForTask(properties.getGraph(),
				this.properties));
		builder.addConstructorArgValue(null);
		builder.addConstructorArgReference("composedTaskStepExecutionListener");

		registry.registerBeanDefinition(taskName + UUID.randomUUID(), builder.getBeanDefinition());
	}
}