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

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.cloud.dataflow.rest.client.TaskOperations;
import org.springframework.cloud.task.app.composedtaskrunner.properties.ComposedTaskProperties;
import org.springframework.cloud.task.repository.TaskExecution;
import org.springframework.cloud.task.repository.TaskExplorer;
import org.springframework.cloud.task.repository.TaskRepository;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.interceptor.DefaultTransactionAttribute;
import org.springframework.transaction.interceptor.TransactionAttribute;
import org.springframework.util.Assert;

/**
 * @author Glenn Renfro
 */
public class ComposedTaskRunnerStepFactory implements FactoryBean<Step> {

	private TaskRepository taskRepository;

	private TaskOperations taskOperations;

	private TaskExplorer taskExplorer;

	private ComposedTaskProperties composedTaskProperties;

	private String taskName;

	private Map<String, String> taskSpecificProps;

	private List<String> arguments;

	private StepBuilderFactory steps;

	StepExecutionListener composedTaskStepExecutionListener;


	public ComposedTaskRunnerStepFactory(TaskRepository taskRepository,
			TaskOperations taskOperations, TaskExplorer taskExplorer,
			ComposedTaskProperties composedTaskProperties, String taskName,
			Map<String, String> taskSpecificProps, List<String> arguments,
			StepBuilderFactory steps,
			StepExecutionListener composedTaskStepExecutionListener) {
		Assert.notNull(taskRepository, "taskRepository must not be null");
		Assert.notNull(taskExplorer, "taskExplorer must not be null");
		Assert.notNull(taskOperations, "taskOperation must not be null");
		Assert.notNull(composedTaskProperties, "composedTaskProperties must not be null");
		Assert.hasText(taskName, "taskName must not be empty nor null");
		Assert.notNull(taskSpecificProps, "taskSpecificProps must not be null");
		Assert.notNull(steps, "steps must not be null");
		Assert.notNull(composedTaskStepExecutionListener, "composedTaskStepExecutionListener must not be null");

		this.taskRepository = taskRepository;
		this.taskOperations = taskOperations;
		this.taskExplorer = taskExplorer;
		this.composedTaskProperties = composedTaskProperties;
		this.taskName = taskName;
		this.taskSpecificProps = taskSpecificProps;
		this.arguments = arguments;
		this.steps = steps;
		this.composedTaskStepExecutionListener = composedTaskStepExecutionListener;
	}

	@Override
	public Step getObject() throws Exception {
		TaskExecution taskExecution = new TaskExecution();
		taskExecution.setStartTime(new Date());
		taskExecution.setArguments(new ArrayList());
		taskExecution = taskRepository.createTaskExecution(taskExecution);
		TaskLauncherTasklet taskLauncherTasklet = new TaskLauncherTasklet(
				String.valueOf(taskExecution.getExecutionId()),
				this.taskOperations, this.taskExplorer,
				this.composedTaskProperties, this.taskName, this.taskSpecificProps,
				this.arguments);
		String stepName = UUID.randomUUID().toString();
		Step step = this.steps.get(stepName)
				.tasklet(taskLauncherTasklet)
				.transactionAttribute(getTransactionAttribute())
				.listener(composedTaskStepExecutionListener)
				.build();
		return step;
	}

	/**
	 * Using the default transaction attribute for the job will cause the
	 * TaskLauncher not to see the latest state in the database but rather
	 * what is in its transaction.  By setting isolation to READ_COMMITTED
	 * The task launcher can see latest state of the db.  Since the changes
	 * to the task execution are done by the tasks.

	 * @return DefaultTransactionAttribute with isolation set to READ_COMMITTED.
	 */
	private TransactionAttribute getTransactionAttribute() {
		DefaultTransactionAttribute defaultTransactionAttribute =
				new DefaultTransactionAttribute();
		defaultTransactionAttribute.setIsolationLevel(
				Isolation.READ_COMMITTED.value());
		return defaultTransactionAttribute;
	}

	@Override
	public Class<?> getObjectType() {
		return Step.class;
	}

	@Override
	public boolean isSingleton() {
		return false;
	}
}
