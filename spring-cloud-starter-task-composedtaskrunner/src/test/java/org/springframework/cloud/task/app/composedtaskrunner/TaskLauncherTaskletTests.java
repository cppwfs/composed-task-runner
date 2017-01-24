/*
 * Copyright 2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.task.app.composedtaskrunner;

import org.junit.runner.RunWith;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.boot.autoconfigure.PropertyPlaceholderAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.EmbeddedDataSourceConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.task.app.composedtaskrunner.properties.ComposedTaskProperties;
import org.springframework.cloud.task.repository.support.TaskRepositoryInitializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes={EmbeddedDataSourceConfiguration.class,
		TaskLauncherTaskletTests.TestConfiguration.class,
		PropertyPlaceholderAutoConfiguration.class})
public class TaskLauncherTaskletTests {
//
//	private static final String TASK_NAME = "testTask1";
//
//	@Autowired
//	private DataSource dataSource;
//
//	@Autowired
//	JobBuilderFactory jobs;
//
//	@Autowired
//	StepBuilderFactory steps;
//
//	@Autowired
//	ComposedTaskProperties composedTaskProperties;
//
//	@Autowired
//	TaskRepositoryInitializer taskRepositoryInitializer;
//
//	@Autowired
//	JobRepository jobRepository;
//
//	private TaskOperations taskOperations;
//
//	private TaskRepository taskRepository;
//
//	private TaskExplorer taskExplorer;
//
//
//	@Before
//	public void setup() throws Exception{
//		taskRepositoryInitializer.setDataSource(dataSource);
//
//		taskRepositoryInitializer.afterPropertiesSet();
//		taskOperations = mock(TaskOperations.class);
//		TaskExecutionDaoFactoryBean taskExecutionDaoFactoryBean =
//				new TaskExecutionDaoFactoryBean(dataSource);
//		taskRepository = new SimpleTaskRepository(taskExecutionDaoFactoryBean);
//		taskExplorer = new SimpleTaskExplorer(taskExecutionDaoFactoryBean);
//	}
//
//	@Test
//	@DirtiesContext
//	public void testTaskLauncherTasklet() throws Exception{
//		TaskLauncherTasklet taskLauncherTasklet =
//				getTaskExecutionTasklet(getCompleteTaskExecution());
//		ChunkContext chunkContext = chunkContext();
//		taskLauncherTasklet.execute(null, chunkContext);
//		assertEquals("1", chunkContext.getStepContext()
//				.getStepExecution().getExecutionContext()
//				.get("task-execution-id"));
//
//		chunkContext = chunkContext();
//		taskLauncherTasklet = getTaskExecutionTasklet(getCompleteTaskExecution());
//		taskLauncherTasklet.execute(null, chunkContext);
//		assertEquals("2", chunkContext.getStepContext()
//				.getStepExecution().getExecutionContext()
//				.get("task-execution-id"));
//	}
//	@Test
//	@DirtiesContext
//	public void testTaskLauncherTaskletTimeout() throws Exception {
//		this.composedTaskProperties.setMaxWaitTime(1000);
//		TaskLauncherTasklet taskLauncherTasklet = getTaskExecutionTasklet();
//		ChunkContext chunkContext = chunkContext();
//		taskLauncherTasklet.execute(null, chunkContext);
//		long taskExecutionId = Long.valueOf((String)chunkContext.getStepContext()
//				.getStepExecution().getExecutionContext()
//				.get("task-execution-id"));
//		assertEquals(1, taskExecutionId);
//		TaskExecution taskExecution = taskExplorer.getTaskExecution(taskExecutionId);
//		assertNull(taskExecution.getExitMessage());
//	}
//		@Test
//	@DirtiesContext
//	public void testInvalidTaskName() throws Exception {
//		String exceptionMessage = null;
//		final String ERROR_MESSAGE =
//				"Could not find task definition named " + TASK_NAME;
//		VndErrors errors = new VndErrors("message", ERROR_MESSAGE, new Link("ref"));
//		Mockito.doThrow(new DataFlowClientException(errors))
//				.when(taskOperations)
//				.launch(Matchers.anyString(),
//						(Map<String, String>) Matchers.any(),
//						(List<String>) Matchers.any());
//		TaskLauncherTasklet taskLauncherTasklet = getTaskExecutionTasklet();
//		ChunkContext chunkContext = chunkContext();
//		try {
//			taskLauncherTasklet.execute(null, chunkContext);
//		}
//		catch (DataFlowClientException dfce) {
//			exceptionMessage = dfce.getMessage();
//		}
//		assertEquals(ERROR_MESSAGE+"\n", exceptionMessage);
//	}
//
//	@Test
//	@DirtiesContext
//	public void testNoDataFlowServer() throws Exception{
//		String exceptionMessage = null;
//		final String ERROR_MESSAGE =
//				"I/O error on GET request for \"http://localhost:9393\": Connection refused; nested exception is java.net.ConnectException: Connection refused";
//		Mockito.doThrow(new ResourceAccessException(ERROR_MESSAGE)).when(taskOperations).launch(Matchers.anyString(), (Map<String,String>) Matchers.any(), (List<String>) Matchers.any());
//		TaskLauncherTasklet taskLauncherTasklet = getTaskExecutionTasklet();
//		ChunkContext chunkContext = chunkContext();
//		try {
//			taskLauncherTasklet.execute(null, chunkContext);
//		}
//		catch (ResourceAccessException rae) {
//			exceptionMessage = rae.getMessage();
//		}
//		assertEquals(ERROR_MESSAGE, exceptionMessage);
//	}
//
//	private TaskExecution getCompleteTaskExecution() {
//		TaskExecution taskExecution = taskRepository.createTaskExecution();
//		taskRepository.completeTaskExecution(taskExecution.getExecutionId(),
//				0, new Date(), "");
//		return taskExecution;
//	}
//
//	private TaskLauncherTasklet getTaskExecutionTasklet() {
//		TaskExecution taskExecution = taskRepository.createTaskExecution();
//		return getTaskExecutionTasklet(taskExecution);
//	}
//
//	private TaskLauncherTasklet getTaskExecutionTasklet(TaskExecution taskExecution) {
//		return new TaskLauncherTasklet(
//				String.valueOf(taskExecution.getExecutionId()), taskOperations,
//				taskExplorer, composedTaskProperties,
//				TASK_NAME, new HashMap<String,String>(), new ArrayList<String>());
//	}
//
//	private ChunkContext chunkContext ()
//	{
//		final long JOB_EXECUTION_ID = 123L;
//		final String STEP_NAME = "myTestStep";
//
//		JobExecution jobExecution = new JobExecution(JOB_EXECUTION_ID);
//		StepExecution stepExecution = new StepExecution(STEP_NAME, jobExecution);
//		StepContext stepContext = new StepContext(stepExecution);
//		ChunkContext chunkContext = new ChunkContext(stepContext);
//		return chunkContext;
//	}
//
	@Configuration
	@EnableBatchProcessing
	@EnableConfigurationProperties(ComposedTaskProperties.class)
	public static class TestConfiguration {

		@Bean
		TaskRepositoryInitializer taskRepositoryInitializer() {
			return new TaskRepositoryInitializer();
		}
	}
}
