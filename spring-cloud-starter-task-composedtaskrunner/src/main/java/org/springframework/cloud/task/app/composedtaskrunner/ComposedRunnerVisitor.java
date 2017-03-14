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

import java.util.Stack;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.cloud.dataflow.core.dsl.ComposedTaskVisitor;
import org.springframework.cloud.dataflow.core.dsl.FlowNode;
import org.springframework.cloud.dataflow.core.dsl.LabelledComposedTaskNode;
import org.springframework.cloud.dataflow.core.dsl.SplitNode;
import org.springframework.cloud.dataflow.core.dsl.TaskAppNode;
import org.springframework.cloud.dataflow.core.dsl.TransitionNode;

/**
 * Creates the Job Flow based on the ComposedTaskVisitor implementation
 * provided by the TaskParser in Spring Cloud Data Flow.
 *
 * @author Glenn Renfro.
 */
public class ComposedRunnerVisitor extends ComposedTaskVisitor {

	private Stack flowStack = new Stack();

	private static final Log logger = LogFactory.getLog(ComposedRunnerVisitor.class);

	/**
	 * The first call made to a visitor.
	 */
	public void startVisit() {
		logger.debug("Start Visit");
	}

	/**
	 * The last call made to a visitor.
	 */
	public void endVisit() {
		logger.debug("End Visit");
	}

	/**
	 * @param firstNode the sequence number, where the primary sequence is 0
	 * @return false to skip visiting the specified sequence
	 */
	public boolean preVisitSequence(LabelledComposedTaskNode firstNode, int sequenceNumber) {
		logger.debug("Pre Visit Sequence");
		return true;
	}

	public void postVisitSequence(LabelledComposedTaskNode firstNode, int sequenceNumber) {
		logger.debug("Post Visit Sequence");
	}

	/**
	 * @param flow the flow which represents things to execute in sequence
	 * @return false to skip visiting this flow
	 */
	public boolean preVisit(FlowNode flow) {
		logger.debug("Pre Visit Flow:  " + flow);
//		flowStack.push(new FlowMarker(FlowMarker.FlowMarkerType.FLOW, flow.toString()));
		flowStack.push(flow);
		return true;
	}

	public void visit(FlowNode flow) {
		logger.debug("Visit Flow:  " + flow);

	}

	public void postVisit(FlowNode flow) {
		logger.debug("Post Visit Flow:  " + flow);
	}

	/**
	 * @param split the split which represents things to execute in parallel
	 * @return false to skip visiting this split
	 */
	public boolean preVisit(SplitNode split) {
		logger.debug("Pre Visit Split:  " + split);
		return true;
	}

	public void visit(SplitNode split) {
		logger.debug("Visit Split:  " + split);
		flowStack.push(split);
	}

	public void postVisit(SplitNode split) {
		logger.debug("Post Visit Split:  " + split);
		flowStack.push(split);

	}

	/**
	 * <b>This preVisit/visit/postVisit sequence for taskApp is not invoked for inlined references to apps
	 * in transitions, for example: <tt>appA 0->:foo 1->appB</tt>. The reference to <tt>appB</tt> would be
	 * seen in the transition visit below.
	 * @param taskApp the use of a task app in a composed task dsl
	 * @return false to skip visiting this taskApp
	 */
	public boolean preVisit(TaskAppNode taskApp) {
		logger.debug("Pre Visit taskApp:  " + taskApp);
		return true;
	}

	public void visit(TaskAppNode taskApp) {
		logger.debug("Visit taskApp:  " + taskApp);
		flowStack.push(taskApp);
	}

	public void postVisit(TaskAppNode taskApp) {
		logger.debug("Post Visit taskApp:  " + taskApp);
	}


	/**
	 * After <tt>visit(TaskApp)</tt> and before <tt>postVisit(TaskApp)</tt> the transitions (if there
	 * are any) are visited for that task app.
	 * @param transition the transition
	 * @return false to skip visiting this transition
	 */
	public boolean preVisit(TransitionNode transition) {
		logger.debug("Pre Visit transition:  " + transition);
		return true;
	}

	public void visit(TransitionNode transition) {
		logger.debug("Visit transition:  " + transition);
	}

	public void postVisit(TransitionNode transition) {
		logger.debug("Post Visit transition:  " + transition);
	}

	public Stack getFlowStack() {
		return flowStack;
	}

	public void setFlowStack(Stack flowStack) {
		this.flowStack = flowStack;
	}
}
