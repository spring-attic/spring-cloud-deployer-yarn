/*
 * Copyright 2015-2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.deployer.spi.yarn;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.cloud.deployer.spi.yarn.YarnCloudAppService.CloudAppType;
import org.springframework.core.io.ResourceLoader;
import org.springframework.core.task.TaskExecutor;
import org.springframework.statemachine.StateContext;
import org.springframework.statemachine.action.Action;
import org.springframework.statemachine.guard.Guard;

/**
 * Statemachine for task launching.
 *
 * @author Janne Valkealahti
 *
 */
public class TaskLauncherStateMachine extends AbstractDeployerStateMachine {

	private static final Logger logger = LoggerFactory.getLogger(TaskLauncherStateMachine.class);
	private final static String MODEL_LOCATION = "classpath:tasklauncher-model.uml";

	public final static String EVENT_LAUNCH = "LAUNCH";
	public final static String EVENT_CANCEL = "CANCEL";

	/**
	 * Instantiates a new task launcher state machine.
	 *
	 * @param yarnCloudAppService the yarn cloud app service
	 * @param taskExecutor the task executor
	 * @param beanFactory the bean factory
	 * @param resourceLoader the resource loader
	 */
	public TaskLauncherStateMachine(YarnCloudAppService yarnCloudAppService, TaskExecutor taskExecutor, BeanFactory beanFactory,
			ResourceLoader resourceLoader) {
		super(yarnCloudAppService, taskExecutor, beanFactory, resourceLoader, MODEL_LOCATION);
	}

	@Override
	protected Map<String, Action<String, String>> getRegisteredActions() {
		HashMap<String, Action<String, String>> actions = new HashMap<>();
		actions.put("resetVariablesAction", new ResetVariablesAction());
		actions.put("launchAction", new LaunchAction());
		actions.put("cancelAction", new CancelAction());
		actions.put("checkAppAction", new CheckAppAction(CloudAppType.TASK));
		actions.put("pushAppAction", new PushAppAction(CloudAppType.TASK));
		actions.put("pushArtifactAction", new PushArtifactAction());
		actions.put("startAppAction", new StartAppAction());
		actions.put("stopAppAction", new StopAppAction());
		actions.put("errorHandlingAction", new ErrorAction());
		return actions;
	}

	@Override
	protected Map<String, Guard<String, String>> getRegisteredGuards() {
		HashMap<String, Guard<String, String>> guards = new HashMap<>();
		guards.put("pushAppGuard", new PushAppGuard());
		guards.put("errorGuard", new ErrorGuard());
		return guards;
	}

	private class LaunchAction implements Action<String, String> {

		@Override
		public void execute(StateContext<String, String> context) {
			context.getExtendedState().getVariables().put(VAR_MESSAGE_ID, context.getMessageHeaders().getId().toString());
		}
	}

	private class CancelAction implements Action<String, String> {

		@Override
		public void execute(StateContext<String, String> context) {
			context.getExtendedState().getVariables().put(VAR_MESSAGE_ID, context.getMessageHeaders().getId().toString());
		}
	}

	private class StartAppAction implements Action<String, String> {

		@Override
		public void execute(StateContext<String, String> context) {
			String appVersion = (String) context.getMessageHeader(HEADER_APP_VERSION);

			// we control type so casting is safe
			@SuppressWarnings("unchecked")
			List<String> contextRunArgs = (List<String>) context.getMessageHeader(HEADER_CONTEXT_RUN_ARGS);

			String applicationId = getYarnCloudAppService().submitApplication(appVersion, CloudAppType.TASK, contextRunArgs);
			logger.info("New application id is {}", applicationId);
			context.getExtendedState().getVariables().put(VAR_APPLICATION_ID, applicationId);
		}
	}

	private class StopAppAction implements Action<String, String> {

		@Override
		public void execute(StateContext<String, String> context) {
			String applicationId = (String) context.getMessageHeader(HEADER_APPLICATION_ID);
			logger.info("Killing application {}", applicationId);
			getYarnCloudAppService().killApplication(applicationId, CloudAppType.TASK);
		}
	}
}
