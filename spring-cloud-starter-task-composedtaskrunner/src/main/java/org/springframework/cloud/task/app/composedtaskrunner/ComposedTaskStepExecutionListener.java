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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.listener.StepExecutionListenerSupport;
import org.springframework.cloud.task.app.composedtaskrunner.properties.ComposedTaskProperties;
import org.springframework.cloud.task.repository.TaskExecution;
import org.springframework.cloud.task.repository.TaskExplorer;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Listener for the TaskLauncherTasklet that waits for the task to complete
 * and sets the appropriate result for this step based on the launched task
 * exit code.
 *
 * @author Glenn Renfro
 */
public class ComposedTaskStepExecutionListener extends StepExecutionListenerSupport{

	private TaskExplorer taskExplorer;

	private ComposedTaskProperties properties;

	private final Logger logger = LoggerFactory.getLogger(ComposedTaskStepExecutionListener.class);

	public ComposedTaskStepExecutionListener(TaskExplorer taskExplorer, ComposedTaskProperties properties) {
		Assert.notNull(taskExplorer, "taskExplorer must not be null.");
		Assert.notNull(properties, "properties must not be null.");
		this.taskExplorer = taskExplorer;
		this.properties = properties;
	}

	/**
	 * Waits for the TaskExecution for the step to complete up until a timeout.
	 * If the timeout as established in the ComposedTaskProperties is exceeded
	 * then the exit status will be UNKNOWN.  If an exitMessage is returned
	 * by the TaskExecution then the exit status returned will be the
	 * ExitStatus.  If no exitMessage is set for the task execution and the
	 * task returns an exitCode ! = to zero an exit status of FAILED is
	 * returned.  If no exit message is set and the exit code of the task is
	 * zero then the ExitStatus of COMPLETED is returned.
	 * @param stepExecution The stepExecution that kicked of the Task.
	 * @return ExitStatus of COMPLETED else FAILED.
	 */
	@Override
	public ExitStatus afterStep(StepExecution stepExecution) {
		ExitStatus result = ExitStatus.COMPLETED;
		logger.info(String.format("AfterStep processing for stepExecution %s",
				stepExecution.getStepName()));

		String executionId = (String)stepExecution.getExecutionContext().get("task-execution-id");
		Assert.hasText(executionId, "TaskLauncherTasklet did not " +
				"return a task-execution-id.  Check to see if task " +
				"exists.");

			TaskExecution resultExecution = taskExplorer.getTaskExecution(Long.valueOf(executionId));
			if(resultExecution.getEndTime() == null) {
				result = ExitStatus.UNKNOWN;
			}
			else if(!StringUtils.isEmpty(resultExecution.getExitMessage())) {
				result = new ExitStatus(resultExecution.getExitMessage());
			}
			else if(resultExecution.getExitCode() != 0) {
				result = ExitStatus.FAILED;
			}
		logger.info(String.format("AfterStep processing complete for " +
						"stepExecution %s with taskExecution %s",
				stepExecution.getStepName(), executionId));
		return result;
	}

}
