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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.dataflow.core.dsl.ComposedTaskVisitor;
import org.springframework.cloud.dataflow.core.dsl.Flow;
import org.springframework.cloud.dataflow.core.dsl.Split;
import org.springframework.cloud.dataflow.core.dsl.TaskApp;
import org.springframework.cloud.dataflow.core.dsl.Transition;
import org.springframework.context.ApplicationContext;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.util.StringUtils;

/**
 * @author Glenn Renfro
 */
public class ComposedRunnerVisitor extends ComposedTaskVisitor {

	@Autowired
	private ApplicationContext context;

	private FlowBuilder<org.springframework.batch.core.job.flow.Flow> flowBuilder;

	private boolean isInitialized = false;

	private boolean isSplit = false;
	private boolean isFirstSplit = false;

	private List<org.springframework.batch.core.job.flow.Flow> flows = new ArrayList<>();;
	private Step currentStep;
	private Step transitionStep;
	private Map<String, Integer> taskBeanSuffixes = new HashMap<>();

	/**
	 * @param sequenceNumber, where the primary sequence is 0
	 * @return false to skip visiting the specified sequence
	 */
	public boolean preVisit(int sequenceNumber) {
		System.out.println("previsit" + sequenceNumber);
		return true;
	}

	public void postVisit(int sequenceNumber) {
		System.out.println("postVisit" + sequenceNumber);
	}

	/**
	 * @param flow the flow which represents things to execute in sequence
	 * @return false to skip visiting this flow
	 */
	public boolean preVisit(Flow flow) {
		System.out.println("previsit Flow " + flow.toString());
		return true;
	}

	public void visit(Flow flow) {
		System.out.println("Visit Flow " + flow.toString());
	}

	public void postVisit(Flow flow) {
		System.out.println("postVisit Flow " + flow.toString());
	}

	/**
	 * @param split the split which represents things to execute in parallel
	 * @return false to skip visiting this split
	 */
	public boolean preVisit(Split split) {
		isSplit = true;
		System.out.println("previsit split " + split.toString());

		return true;
	}

	public void visit(Split split) {
		System.out.println("visit split " + split.toString());

	}

	public void postVisit(Split split) {
		org.springframework.batch.core.job.flow.Flow splitFlow =
				new FlowBuilder.SplitBuilder<>(
				new FlowBuilder<org.springframework.batch.core.job.flow.Flow>(UUID.randomUUID().toString()),
				new SimpleAsyncTaskExecutor())
				.add(flows.toArray(new org.springframework.batch.core.job.flow.Flow[flows.size()])).build();
		if(isFirstSplit) {
			flowBuilder.start(splitFlow);
		}
		else {
			flowBuilder.next(splitFlow);
		}
		isSplit = false;
		isFirstSplit = false;
		System.out.println("postvisit split " + split.toString());
		flows = new ArrayList<>();
	}


	/**
	 * <b>This preVisit/visit/postVisit sequence for taskApp is not invoked for inlined references to apps
	 * in transitions, for example: <tt>appA 0->:foo 1->appB</tt>. The reference to <tt>appB</tt> would be
	 * seen in the transition visit below.
	 * @param taskApp the use of a task app in a composed task dsl
	 * @return false to skip visiting this taskApp
	 */
	public boolean preVisit(TaskApp taskApp) {
		System.out.println("previsit taskApp " + taskApp.getName());
		return true;
	}

	public void visit(TaskApp taskApp) {
		String taskName;
		boolean isLabel = false;
		if(taskApp.getLabel() != null) {
			taskName = taskApp.getLabel().stringValue();
			isLabel = true;
		}
		else {
			taskName = taskApp.getName();
		}
		currentStep = context.getBean(getBeanName(taskName, isLabel), Step.class);
		if(!isInitialized) {
			initialize();
			if(isSplit) {
				isFirstSplit = true;
				flows.add(new FlowBuilder<org.springframework.batch.core.job.flow.Flow>(getBeanName(taskName, isLabel))
						.from(currentStep).end());
			}
			else {
				flowBuilder
						.start(currentStep);
			}
		} else {
			if(isSplit) {
				flows.add(new FlowBuilder<org.springframework.batch.core.job.flow.Flow>(getBeanName(taskName, isLabel))
						.from(currentStep).end());
			}
			else {
				flowBuilder.next(currentStep);
			}
		}
		System.out.println("visit taskapp " + taskApp.toString());

	}

	public void postVisit(TaskApp taskApp) {
		if (transitionStep != null) {
			flowBuilder.from(transitionStep);
		}
		transitionStep = null;
		System.out.println("postvisit taskapp " + taskApp.getName());
	}

	public boolean preVisit(Transition transition) {
		System.out.println("previsit transition " + transition.getTargetApp());
		return true;
	}

	public void visit(Transition transition) {
		System.out.println("visit transition " + transition.getTargetApp());
		String targetName;
		boolean isLabel = false;
		if(transition.getTargetLabel() != null) {
			targetName = transition.getTargetLabel();
			isLabel = true;
		}
		else {
			targetName = transition.getTargetApp();
		}
		transitionStep = context.getBean(getBeanName(targetName, isLabel), Step.class);
		flowBuilder.on(transition.getStatusToCheck())
				.to(transitionStep);
		flowBuilder.from(currentStep);
	}

	public void postVisit(Transition transition) {
		System.out.println("postvisit transition " + transition.getTargetApp());
	}

	public FlowBuilder<org.springframework.batch.core.job.flow.Flow> getFlowBuilder() {
		return flowBuilder;
	}

	private void initialize() {
		if(!isInitialized) {
			flowBuilder = new FlowBuilder<>(UUID.randomUUID().toString());
			isInitialized = true;
		}
	}

	private String getBeanName(String taskName, boolean isLabel) {
		Integer taskSuffix;
		if(isLabel) {
			return taskName;
		}
		if(taskName.contains("->")) {
			taskName = taskName.substring(taskName.indexOf("->")+2);
		}
		if(taskBeanSuffixes.containsKey(taskName)) {
			taskSuffix = taskBeanSuffixes.get(taskName);
		}
		else {
			taskSuffix = 0;
		}
		String result = String.format("%s_%s", taskName, taskSuffix++);
		taskBeanSuffixes.put(taskName, taskSuffix);
		return result;
	}
}
