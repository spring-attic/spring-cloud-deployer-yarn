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
import org.springframework.cloud.deployer.spi.yarn.YarnCloudAppService.CloudAppInstanceInfo;
import org.springframework.cloud.deployer.spi.yarn.YarnCloudAppService.CloudAppType;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.core.task.TaskExecutor;
import org.springframework.statemachine.StateContext;
import org.springframework.statemachine.action.Action;
import org.springframework.statemachine.guard.Guard;
import org.springframework.util.StringUtils;

/**
 * Statemachine for app deployments.
 *
 * @author Janne Valkealahti
 *
 */
public class AppDeployerStateMachine extends AbstractDeployerStateMachine {

	private static final Logger logger = LoggerFactory.getLogger(AppDeployerStateMachine.class);

	private final static String MODEL_LOCATION = "classpath:appdeployer-model.uml";

	public final static String EVENT_DEPLOY = "DEPLOY";
	public final static String EVENT_UNDEPLOY = "UNDEPLOY";

	public static final String VAR_INSTANCE_ADDRESS = "instanceAddress";
	public static final String VAR_COUNT = "count";
	public static final String VAR_APPNAME = "appname";
	public static final String VAR_CLUSTER_ID = "clusterId";
	public static final String VAR_DEFINITION_PARAMETERS = "definitionParameters";

	public static final String HEADER_CLUSTER_ID = "clusterId";
	public static final String HEADER_COUNT = "count";
	public static final String HEADER_DEFINITION_PARAMETERS = "definitionParameters";

	/**
	 * Instantiates a new app deployer state machine.
	 *
	 * @param yarnCloudAppService the yarn cloud app service
	 * @param taskExecutor the task executor
	 * @param beanFactory the bean factory
	 * @param resourceLoader the resource loader
	 */
	public AppDeployerStateMachine(YarnCloudAppService yarnCloudAppService, TaskExecutor taskExecutor, BeanFactory beanFactory,
			ResourceLoader resourceLoader) {
		super(yarnCloudAppService, taskExecutor, beanFactory, resourceLoader, MODEL_LOCATION);
	}

	@Override
	protected Map<String, Action<String, String>> getRegisteredActions() {
		HashMap<String, Action<String, String>> actions = new HashMap<>();
		actions.put("resetVariablesAction", new ResetVariablesAction());
		actions.put("deployAction", new DeployAction());
		actions.put("checkAppAction", new CheckAppAction(CloudAppType.STREAM));
		actions.put("pushAppAction", new PushAppAction(CloudAppType.STREAM));
		actions.put("checkInstanceAction", new CheckInstanceAction());
		actions.put("pushArtifactAction", new PushArtifactAction());
		actions.put("startInstanceAction", new StartInstanceAction());
		actions.put("waitInstanceAction", new WaitInstanceAction());
		actions.put("resolveInstanceAction", new ResolveInstanceAction());
		actions.put("createClusterAction", new CreateClusterAction());
		actions.put("startClusterAction", new StartClusterAction());
		actions.put("stopClusterAction", new StopClusterAction());
		actions.put("destroyClusterAction", new DestroyClusterAction());
		actions.put("errorHandlingAction", new ErrorAction());
		return actions;
	}

	@Override
	protected Map<String, Guard<String, String>> getRegisteredGuards() {
		HashMap<String, Guard<String, String>> guards = new HashMap<>();
		guards.put("pushAppGuard", new PushAppGuard());
		guards.put("startInstanceGuard", new StartInstanceGuard());
		guards.put("errorGuard", new ErrorGuard());
		guards.put("instanceGuard", new InstanceGuard());
		return guards;
	}

	/**
	 * {@link Action} which queries {@link YarnCloudAppService} for existing
	 * running instances.
	 */
	private class CheckInstanceAction implements Action<String, String> {

		@Override
		public void execute(StateContext<String, String> context) {
			String appVersion = (String) context.getMessageHeader(HEADER_APP_VERSION);
			String groupId = (String) context.getMessageHeader(HEADER_GROUP_ID);
			String appName = "scdstream:" + appVersion + ":" + groupId;
			CloudAppInstanceInfo appInstanceInfo = findRunningInstance(appName);
			if (appInstanceInfo != null) {
				context.getExtendedState().getVariables().put(VAR_APPLICATION_ID, appInstanceInfo.getApplicationId());
			}
		}
	}

	private CloudAppInstanceInfo findRunningInstance(String appName) {
		for (CloudAppInstanceInfo appInstanceInfo : getYarnCloudAppService().getInstances(CloudAppType.STREAM)) {
			logger.info("Checking instance {} for appName {}", appInstanceInfo, appName);
			if (appInstanceInfo.getName().equals(appName) && appInstanceInfo.getState().equals("RUNNING")
					&& appInstanceInfo.getAddress().contains("http")) {
				logger.info("Using instance {}", appInstanceInfo);
				return appInstanceInfo;
			}
		}
		return null;
	}

	private class StartInstanceAction implements Action<String, String> {

		@Override
		public void execute(StateContext<String, String> context) {
			String appVersion = (String) context.getMessageHeader(HEADER_APP_VERSION);

			// we control type so casting is safe
			@SuppressWarnings("unchecked")
			List<String> contextRunArgs = (List<String>) context.getMessageHeader(HEADER_CONTEXT_RUN_ARGS);
			String applicationId = getYarnCloudAppService().submitApplication(appVersion, CloudAppType.STREAM, contextRunArgs);
			context.getExtendedState().getVariables().put(VAR_APPLICATION_ID, applicationId);
		}
	}

	private class WaitInstanceAction implements Action<String, String> {

		@Override
		public void execute(StateContext<String, String> context) {
			String appVersion = (String) context.getMessageHeader(HEADER_APP_VERSION);
			String groupId = (String) context.getMessageHeader(HEADER_GROUP_ID);
			if (StringUtils.hasText(appVersion) && StringUtils.hasText(groupId)) {
				String appName = "scdstream:" + appVersion + ":" + groupId;
				context.getExtendedState().getVariables().put(VAR_APPNAME, appName);
			}
		}
	}

	private class ResolveInstanceAction implements Action<String, String> {

		@Override
		public void execute(StateContext<String, String> context) {
			String appName = context.getExtendedState().get(VAR_APPNAME, String.class);
			String applicationId = context.getExtendedState().get(VAR_APPLICATION_ID, String.class);
			CloudAppInstanceInfo appInstanceInfo = findRunningInstance(appName);
			if (appInstanceInfo != null && appInstanceInfo.getApplicationId().equals(applicationId)) {
				context.getExtendedState().getVariables().put(VAR_INSTANCE_ADDRESS, appInstanceInfo.getAddress());
			}
		}
	}

	private class CreateClusterAction implements Action<String, String> {

		@SuppressWarnings("unchecked")
		@Override
		public void execute(StateContext<String, String> context) {
			Resource artifact = context.getExtendedState().get(VAR_ARTIFACT, Resource.class);
			getYarnCloudAppService().createCluster(
					context.getExtendedState().get(VAR_APPLICATION_ID, String.class),
					context.getExtendedState().get(HEADER_CLUSTER_ID, String.class),
					context.getExtendedState().get(VAR_COUNT, Integer.class),
					artifact != null ? artifact.getFilename() : null,
					context.getExtendedState().get(HEADER_DEFINITION_PARAMETERS, Map.class));
		}
	}

	private class StartClusterAction implements Action<String, String> {

		@Override
		public void execute(StateContext<String, String> context) {
			getYarnCloudAppService().startCluster(context.getExtendedState().get(VAR_APPLICATION_ID, String.class), context
					.getExtendedState().get(HEADER_CLUSTER_ID, String.class));
		}
	}

	private class DeployAction implements Action<String, String> {

		@Override
		public void execute(StateContext<String, String> context) {
			Integer count = context.getMessageHeaders().get(HEADER_COUNT, Integer.class);
			String clusterId = context.getMessageHeaders().get(HEADER_CLUSTER_ID, String.class);
			Map<?, ?> definitionParameters = context.getMessageHeaders().get(HEADER_DEFINITION_PARAMETERS, Map.class);
			Resource artifact = context.getMessageHeaders().get(HEADER_ARTIFACT, Resource.class);
			context.getExtendedState().getVariables().put(VAR_COUNT, count != null ? count : 1);
			context.getExtendedState().getVariables().put(VAR_CLUSTER_ID, clusterId);
			context.getExtendedState().getVariables().put(VAR_DEFINITION_PARAMETERS, definitionParameters);
			if (artifact != null) {
				context.getExtendedState().getVariables().put(VAR_ARTIFACT, artifact);
			}
			context.getExtendedState().getVariables().put(VAR_MESSAGE_ID, context.getMessageHeaders().getId().toString());
		}
	}

	private class StartInstanceGuard implements Guard<String, String> {

		@Override
		public boolean evaluate(StateContext<String, String> context) {
			return !context.getExtendedState().getVariables().containsKey(VAR_APPLICATION_ID);
		}
	}

	private class StopClusterAction implements Action<String, String> {

		@Override
		public void execute(StateContext<String, String> context) {
			String clusterId = context.getMessageHeaders().get(HEADER_CLUSTER_ID, String.class);
			String applicationId = (String) context.getMessageHeader(HEADER_APPLICATION_ID);
			getYarnCloudAppService().stopCluster(applicationId, clusterId);
		}
	}

	private class DestroyClusterAction implements Action<String, String> {

		@Override
		public void execute(StateContext<String, String> context) {
			String clusterId = context.getMessageHeaders().get(HEADER_CLUSTER_ID, String.class);
			String applicationId = (String) context.getMessageHeader(HEADER_APPLICATION_ID);
			getYarnCloudAppService().destroyCluster(applicationId, clusterId);
		}
	}

	private class InstanceGuard implements Guard<String, String> {

		@Override
		public boolean evaluate(StateContext<String, String> context) {
			return context.getExtendedState().getVariables().containsKey(VAR_INSTANCE_ADDRESS);
		}
	}
}
