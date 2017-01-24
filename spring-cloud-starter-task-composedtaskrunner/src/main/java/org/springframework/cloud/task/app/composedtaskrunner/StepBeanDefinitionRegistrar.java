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

import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.cloud.task.app.composedtaskrunner.properties.PropertyUtility;
import org.springframework.context.EnvironmentAware;
import org.springframework.context.annotation.ImportBeanDefinitionRegistrar;
import org.springframework.core.env.Environment;
import org.springframework.core.type.AnnotationMetadata;

/**
 * Created by glennrenfro on 1/24/17.
 */
public class StepBeanDefinitionRegistrar implements ImportBeanDefinitionRegistrar, EnvironmentAware {

	private Environment env;

	@Override
	public void registerBeanDefinitions(AnnotationMetadata importingClassMetadata, BeanDefinitionRegistry registry) {

		System.out.println("env = " + env);

		BeanDefinitionBuilder builder = BeanDefinitionBuilder.rootBeanDefinition(ComposedTaskRunnerStepFactory.class);

		builder.addConstructorArgReference("taskRepository");
		builder.addConstructorArgReference("taskOperations");
		builder.addConstructorArgReference("taskExplorer");
		builder.addConstructorArgValue(this.env);
		String taskName = this.env.getProperty("graph");
		System.out.println(">> graph = " + taskName);
		builder.addConstructorArgValue(taskName);
		builder.addConstructorArgValue(
				PropertyUtility.getPropertiesForTask(taskName,
				this.env));
		builder.addConstructorArgValue(null);
		builder.addConstructorArgReference("composedTaskStepExecutionListener");

//		registry.registerBeanDefinition(taskName + UUID.randomUUID(), builder.getBeanDefinition());
		registry.registerBeanDefinition("foo" + UUID.randomUUID(), builder.getBeanDefinition());
	}

	@Override
	public void setEnvironment(Environment environment) {
		this.env = environment;
	}
}