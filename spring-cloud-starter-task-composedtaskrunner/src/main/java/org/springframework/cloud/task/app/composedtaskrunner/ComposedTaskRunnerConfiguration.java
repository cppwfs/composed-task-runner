/*
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

import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.dataflow.rest.client.TaskOperations;
import org.springframework.cloud.task.app.composedtaskrunner.properties.ComposedTaskProperties;
import org.springframework.cloud.task.app.composedtaskrunner.properties.PropertyUtility;
import org.springframework.cloud.task.configuration.EnableTask;
import org.springframework.cloud.task.repository.TaskExplorer;
import org.springframework.cloud.task.repository.TaskRepository;
import org.springframework.cloud.task.repository.support.SimpleTaskExplorer;
import org.springframework.cloud.task.repository.support.SimpleTaskRepository;
import org.springframework.cloud.task.repository.support.TaskExecutionDaoFactoryBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Configures the Job that will execute the Composed Task Execution.
 *
 * @author Glenn Renfro
 */
@Configuration
@EnableBatchProcessing
@EnableTask
@EnableConfigurationProperties(ComposedTaskProperties.class)
public class ComposedTaskRunnerConfiguration {

	@Autowired
	private JobBuilderFactory jobs;

	@Autowired
	private StepBuilderFactory steps;

	@Autowired
	private DataSource dataSource;

	@Autowired
	private ComposedTaskProperties properties;

	@Autowired
	private TaskOperations taskOperations;

	@Autowired
	private ApplicationContext context;

	@Bean
	public Job job() throws Exception {
		//TODO add parser generation of batch flow here
		FlowBuilder<Flow> flowBuilder = new FlowBuilder<Flow>(UUID.randomUUID().toString())
				.start(getStep(properties.getGraph(),
						PropertyUtility.getPropertiesForTask(properties.getGraph(),
								this.properties), null));//for now assumes single step until we get parser to build out the flow.
		return this.jobs.get(
				properties.getJobNamePrefix() + UUID.randomUUID().toString())
				.start(flowBuilder.end()).end().build();
	}

	@Bean
	public TaskRepository taskRepository() {
		return new SimpleTaskRepository(new TaskExecutionDaoFactoryBean(dataSource));
	}

	@Bean
	public TaskExplorer taskExplorer() {
		return new SimpleTaskExplorer(new TaskExecutionDaoFactoryBean(dataSource));
	}

	@Bean
	public StepExecutionListener composedTaskStepExecutionListener() {
		return new ComposedTaskStepExecutionListener(taskExplorer(), properties);
	}

	/**
	 * Creates the step that will launch the TaskLauncherTasklet.
	 * @param taskName - The name of the task definition to launch.
	 * @param taskSpecificProps - the properties required for the task to be launched.
	 * @param arguments - The command line arguments to be passed to the task.
	 * @return The step that will be added to the job.
	 */
	public Step getStep(String taskName, Map<String, String> taskSpecificProps,
			List<String> arguments ) throws Exception {

		return stepFactory(taskName, taskSpecificProps,arguments).getObject();
	}

	public ComposedTaskRunnerStepFactory stepFactory(String taskName,
			Map<String, String> taskSpecificProps, List<String> arguments) {
		return new ComposedTaskRunnerStepFactory(taskRepository(), this.taskOperations,
				taskExplorer(), this.properties, taskName, taskSpecificProps,
				arguments, this.steps, composedTaskStepExecutionListener());
	}

}
