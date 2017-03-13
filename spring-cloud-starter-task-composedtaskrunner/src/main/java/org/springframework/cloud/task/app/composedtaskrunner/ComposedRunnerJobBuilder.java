package org.springframework.cloud.task.app.composedtaskrunner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.dataflow.core.dsl.FlowNode;
import org.springframework.cloud.dataflow.core.dsl.SplitNode;
import org.springframework.cloud.dataflow.core.dsl.TaskAppNode;
import org.springframework.cloud.dataflow.core.dsl.TransitionNode;
import org.springframework.context.ApplicationContext;
import org.springframework.core.task.SimpleAsyncTaskExecutor;

/**
 *  Genererates a Composed Task Job Flow
 *
 * @author Glenn Renfro
 */
public class ComposedRunnerJobBuilder {

	private FlowBuilder<Flow> flowBuilder;

	private boolean isInitialized = false;

	private Map<String, Integer> taskBeanSuffixes = new HashMap<>();

	private static final Log logger = LogFactory.getLog(
			ComposedRunnerJobBuilder.class);

	@Autowired
	private ApplicationContext context;

	private Stack flowStack = new Stack();

	private Stack<Flow> jobStack = new Stack();

	public ComposedRunnerJobBuilder(ComposedRunnerVisitor composedRunnerVisitor) {
		if(!isInitialized) {
			flowBuilder = new FlowBuilder<>(UUID.randomUUID().toString());
			isInitialized = true;
		}
		flowStack = composedRunnerVisitor.getFlowStack();
	}


	public FlowBuilder<Flow> getFlowBuilder() {
		return flowBuilder.start(createFlow());
	}

	private Flow createFlow() {
		Stack<Flow> executionStack = new Stack();
		while(!flowStack.isEmpty()) {
			logger.debug(flowStack.peek());
			if(flowStack.peek() instanceof TaskAppNode) {
				TaskAppNode taskAppNode = (TaskAppNode)flowStack.pop();
				if(taskAppNode.hasTransitions()) {
					handleTransition(executionStack, taskAppNode);
				}
				else {
					executionStack.push(
							getTaskAppFlow(taskAppNode));
				}
			}
			else if(flowStack.peek() instanceof SplitNode) {
				handleSplit(executionStack, (SplitNode)flowStack.pop());
			}
			else if(flowStack.peek() instanceof FlowNode) {
				handleFlow(executionStack);
			}
		}
		return jobStack.pop();
	}

	private void handleFlow(Stack<Flow> executionStack) {
		boolean isFirst = true;
		while(!executionStack.isEmpty()) {
			if(isFirst) {
				flowBuilder.start(executionStack.pop());
				isFirst = false;
			}
			else {
				flowBuilder.next(executionStack.pop());
			}
		}
		flowStack.pop();
		jobStack.push(flowBuilder.end());
	}

	private void handleSplit(Stack<Flow> executionStack, SplitNode splitNode) {
		FlowBuilder<Flow> taskAppFlowBuilder =
				new FlowBuilder("Flow" + UUID.randomUUID().toString());
		List<Flow> flows = new ArrayList<>();
		while (!executionStack.isEmpty()){
			flows.add(executionStack.pop());
		}
		Flow splitFlow = new FlowBuilder.SplitBuilder<>(
						new FlowBuilder<Flow>("Split"+UUID.randomUUID().toString()),
						new SimpleAsyncTaskExecutor())
						.add(flows.toArray(new Flow[flows.size()])).build();

		executionStack.push(taskAppFlowBuilder.next(splitFlow).end());
	}

	private void handleTransition(Stack<Flow> executionStack, TaskAppNode taskAppNode) {
		String beanName = getBeanName(taskAppNode);
		Step currentStep = context.getBean(beanName, Step.class);
		FlowBuilder<Flow> builder = new FlowBuilder<Flow>(beanName).from(currentStep);
		boolean transitionGloballyHandled = false;
		for(TransitionNode transitionNode :taskAppNode.getTransitions()) {
			String transitionBeanName = getBeanName(transitionNode);
			if(transitionNode.getStatusToCheck().equals("*")) {
				transitionGloballyHandled = true;
			}
			Step transitionStep = context.getBean(transitionBeanName, Step.class);
			builder.on(transitionNode.getStatusToCheck()).to(transitionStep).from(currentStep);
		}
		if(transitionGloballyHandled && !executionStack.isEmpty()) {
			executionStack.clear();
		}
		else {
			if(!jobStack.isEmpty()) {
				builder.next(jobStack.pop());
			}
		}
		executionStack.push(builder.end());
	}

	private String getBeanName(TransitionNode transition) {
		if(transition.getTargetLabel() != null) {
			return transition.getTargetLabel();
		}
		return getBeanName(transition.getTargetApp());
	}


	private String getBeanName(TaskAppNode taskApp) {
		if(taskApp.getLabel() != null) {
			return taskApp.getLabel().stringValue();
		}
		String taskName = taskApp.getName();
		if(taskName.contains("->")) {
			taskName = taskName.substring(taskName.indexOf("->")+2);
		}
		return getBeanName(taskName);
	}


	private String getBeanName(String taskName) {
		Integer taskSuffix;
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

	private Flow getTaskAppFlow(TaskAppNode taskApp) {
		String beanName = getBeanName(taskApp);
		Step currentStep = context.getBean(beanName, Step.class);
		return new FlowBuilder<Flow>(beanName).from(currentStep).end();
	}

}
