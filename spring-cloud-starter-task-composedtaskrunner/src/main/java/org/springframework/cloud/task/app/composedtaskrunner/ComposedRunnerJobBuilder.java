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
import org.springframework.cloud.dataflow.core.dsl.ComposedTaskParser;
import org.springframework.cloud.dataflow.core.dsl.FlowNode;
import org.springframework.cloud.dataflow.core.dsl.LabelledComposedTaskNode;
import org.springframework.cloud.dataflow.core.dsl.SplitNode;
import org.springframework.cloud.dataflow.core.dsl.TaskAppNode;
import org.springframework.cloud.dataflow.core.dsl.TaskParser;
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

	private Stack visitorStack = new Stack();

	private Stack<Flow> jobStack = new Stack();

	private Map<String, Stack<Flow>> flowMap;

	public ComposedRunnerJobBuilder(ComposedRunnerVisitor composedRunnerVisitor) {
		if(!isInitialized) {
			flowBuilder = new FlowBuilder<>(UUID.randomUUID().toString());
			isInitialized = true;
		}
		visitorStack = composedRunnerVisitor.getFlowStack();
		flowMap = new HashMap<>();
	}


	public FlowBuilder<Flow> getFlowBuilder() {
		return flowBuilder.start(createFlow());
	}

	private Flow createFlow() {
		Stack<Flow> executionStack = new Stack();
		while(!visitorStack.isEmpty()) {
			logger.debug(visitorStack.peek());
			if(visitorStack.peek() instanceof TaskAppNode) {
				TaskAppNode taskAppNode = (TaskAppNode) visitorStack.pop();
				if(taskAppNode.hasTransitions()) {
					handleTransition(executionStack, taskAppNode);
				}
				else {
					executionStack.push(
							getTaskAppFlow(taskAppNode));
				}
			}
			else if(visitorStack.peek() instanceof SplitNode) {
				handleSplit(executionStack, (SplitNode) visitorStack.pop());
			}
			else if(visitorStack.peek() instanceof FlowNode) {
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
		visitorStack.pop();
		jobStack.push(flowBuilder.end());
	}

	private void handleFlowForSplit(Stack<Flow> executionStack, Stack<Flow> resultStack) {
		boolean isFirst = true;
		FlowBuilder<Flow> localTaskAppFlowBuilder =
				new FlowBuilder("Flow" + UUID.randomUUID().toString());
		while(!executionStack.isEmpty()) {
			if(isFirst) {
				localTaskAppFlowBuilder.start(executionStack.pop());
				isFirst = false;
			}
			else {
				localTaskAppFlowBuilder.next(executionStack.pop());
			}
		}
		resultStack.push(localTaskAppFlowBuilder.end());
	}

	private void handleSplit(Stack<Flow> executionStack, SplitNode splitNode) {
		FlowBuilder<Flow> taskAppFlowBuilder =
				new FlowBuilder("Flow" + UUID.randomUUID().toString());
		List<Flow> flows = new ArrayList<>();
		for(LabelledComposedTaskNode splitElement : splitNode.getSeries()) {
			Stack<Flow> elementFlowStack = processSplitFlow(splitElement);
			while (!elementFlowStack.isEmpty()) {
				flows.add(elementFlowStack.pop());
			}
		}
		Flow splitFlow = new FlowBuilder.SplitBuilder<>(
						new FlowBuilder<Flow>("Split"+UUID.randomUUID().toString()),
						new SimpleAsyncTaskExecutor())
						.add(flows.toArray(new Flow[flows.size()])).build();
		while(!(visitorStack.peek() instanceof SplitNode)) {
			visitorStack.pop();
		}
		visitorStack.pop(); // pop the visitor stack
		executionStack.push(taskAppFlowBuilder.start(splitFlow).end());
	}

	private Stack<Flow> processSplitFlow(LabelledComposedTaskNode node) {
		ComposedTaskParser taskParser = new ComposedTaskParser();
		ComposedRunnerVisitor splitElementVisitor = new ComposedRunnerVisitor();
		taskParser.parse("aname", node.stringify()).accept(splitElementVisitor);
		Stack splitElmentStack = splitElementVisitor.getFlowStack();
		Stack<Flow> elementFlowStack = new Stack<>();
		Stack<Flow> resultFlowStack = new Stack<>();
		while(!splitElmentStack.isEmpty()) {
			System.out.println(splitElmentStack.peek());
			if(splitElmentStack.peek() instanceof TaskAppNode) {
				TaskAppNode taskAppNode = (TaskAppNode) splitElmentStack.pop();
				if(taskAppNode.hasTransitions()) {
					handleTransition(elementFlowStack, taskAppNode);
				}
				else {
					elementFlowStack.push(
							getTaskAppFlow(taskAppNode));
				}
			}
			else if(splitElmentStack.peek() instanceof FlowNode) {
				handleFlowForSplit(elementFlowStack, resultFlowStack);
				splitElmentStack.pop();
			}
		}
		return resultFlowStack;
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
