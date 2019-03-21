/*
 * Copyright 2016 the original author or authors.
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

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.cloud.deployer.spi.yarn.YarnCloudAppService.CloudAppInfo;
import org.springframework.cloud.deployer.spi.yarn.YarnCloudAppService.CloudAppType;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.core.task.TaskExecutor;
import org.springframework.statemachine.StateContext;
import org.springframework.statemachine.StateMachine;
import org.springframework.statemachine.action.Action;
import org.springframework.statemachine.config.StateMachineBuilder;
import org.springframework.statemachine.config.StateMachineBuilder.Builder;
import org.springframework.statemachine.guard.Guard;
import org.springframework.statemachine.uml.UmlStateMachineModelFactory;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Shared base functionality for state machines handling app deployment
 * and task launching.
 *
 * @author Janne Valkealahti
 *
 */
public abstract class AbstractDeployerStateMachine {

	private static final Logger logger = LoggerFactory.getLogger(AbstractDeployerStateMachine.class);

	// common states
	public final static String STATE_READY = "READY";
	public final static String STATE_ERROR = "ERROR";

	// common extended state variables
	public static final String VAR_ERROR = "error";
	public static final String VAR_APP_VERSION = "appVersion";
	public static final String VAR_APPLICATION_ID = "applicationId";
	public static final String VAR_ARTIFACT = "artifact";
	public static final String VAR_MESSAGE_ID = "messageId";

	// common event headers
	public static final String HEADER_APPLICATION_ID = "applicationId";
	public static final String HEADER_APP_VERSION = "appVersion";
	public static final String HEADER_GROUP_ID = "groupId";
	public static final String HEADER_ERROR = "error";
	public static final String HEADER_ARTIFACT = "artifact";
	public static final String HEADER_ARTIFACT_DIR = "artifactDir";
	public static final String HEADER_CONTEXT_RUN_ARGS = "contextRunArgs";
	public static final String HEADER_DEFINITION_PARAMETERS = "definitionParameters";

	private final YarnCloudAppService yarnCloudAppService;
	private final TaskExecutor taskExecutor;
	private final BeanFactory beanFactory;
	private final ResourceLoader resourceLoader;
	private final String modelLocation;

	private boolean autoStart = true;

	/**
	 * Instantiates a new abstract deployer state machine.
	 *
	 * @param yarnCloudAppService the yarn cloud app service
	 * @param taskExecutor the task executor
	 * @param beanFactory the bean factory
	 * @param resourceLoader the resource loader
	 * @param modelLocation the model location
	 */
	public AbstractDeployerStateMachine(YarnCloudAppService yarnCloudAppService, TaskExecutor taskExecutor, BeanFactory beanFactory,
			ResourceLoader resourceLoader, String modelLocation) {
		Assert.notNull(yarnCloudAppService, "YarnCloudAppService must be set");
		Assert.notNull(taskExecutor, "TaskExecutor must be set");
		Assert.notNull(beanFactory, "BeanFactory must be set");
		Assert.notNull(resourceLoader, "ResourceLoader must be set");
		Assert.notNull(modelLocation, "Model must be set");
		this.yarnCloudAppService = yarnCloudAppService;
		this.taskExecutor = taskExecutor;
		this.beanFactory = beanFactory;
		this.resourceLoader = resourceLoader;
		this.modelLocation = modelLocation;
	}

	/**
	 * Builds the state machine.
	 *
	 * @return the state machine
	 * @throws Exception the exception
	 */
	public final StateMachine<String, String> buildStateMachine() throws Exception {
		Builder<String, String> builder = StateMachineBuilder.builder();

		builder.configureConfiguration()
			.withConfiguration()
				.autoStartup(autoStart)
				.taskExecutor(taskExecutor)
				.beanFactory(beanFactory);

		UmlStateMachineModelFactory modelFactory = new UmlStateMachineModelFactory(modelLocation);

		Map<String, Action<String, String>> registeredActions = getRegisteredActions();
		if (registeredActions != null) {
			for (Map.Entry<String, Action<String, String>> entry : registeredActions.entrySet()) {
				// wraps actions to catch errors
				modelFactory.registerAction(entry.getKey(), new ExceptionCatchingAction(entry.getValue()));
			}
		}

		Map<String, Guard<String, String>> registeredGuards = getRegisteredGuards();
		if (registeredGuards != null) {
			for (Map.Entry<String, Guard<String, String>> entry : registeredGuards.entrySet()) {
				modelFactory.registerGuard(entry.getKey(), entry.getValue());
			}
		}

		modelFactory.setResourceLoader(resourceLoader);
		builder.configureModel()
			.withModel()
				.factory(modelFactory);

		return builder.build();
	}

	/**
	 * Sets the auto start flag for builder.
	 *
	 * @param autoStart the new auto start
	 */
	public void setAutoStart(boolean autoStart) {
		this.autoStart = autoStart;
	}

	/**
	 * Gets the registered actions.
	 *
	 * @return the registered actions
	 */
	protected abstract Map<String, Action<String, String>> getRegisteredActions();

	/**
	 * Gets the registered guards.
	 *
	 * @return the registered guards
	 */
	protected abstract Map<String, Guard<String, String>> getRegisteredGuards();

	/**
	 * Gets the yarn cloud app service.
	 *
	 * @return the yarn cloud app service
	 */
	protected YarnCloudAppService getYarnCloudAppService() {
		return yarnCloudAppService;
	}


	/**
	 * {@link Action} which clears existing extended state variables.
	 */
	protected class ResetVariablesAction implements Action<String, String> {

		@Override
		public void execute(StateContext<String, String> context) {
			context.getExtendedState().getVariables().clear();
		}
	}

	/**
	 * {@link Action} which queries {@link YarnCloudAppService} and checks if
	 * passed {@code appVersion} from event headers exists and sends {@code ERROR}
	 * event into state machine if it doesn't exist. Add to be used {@code appVersion}
	 * into extended state variables which later used by other guards and actions.
	 */
	protected class CheckAppAction implements Action<String, String> {

		private final CloudAppType cloudAppType;

		public CheckAppAction(CloudAppType cloudAppType) {
			this.cloudAppType = cloudAppType;
		}

		@Override
		public void execute(StateContext<String, String> context) {
			String appVersion = (String) context.getMessageHeader(HEADER_APP_VERSION);

			if (!StringUtils.hasText(appVersion)) {
				context.getExtendedState().getVariables().put(VAR_ERROR, new RuntimeException("appVersion not defined"));
			} else {
				Collection<CloudAppInfo> appInfos = yarnCloudAppService.getApplications(cloudAppType);
				for (CloudAppInfo appInfo : appInfos) {
					if (appInfo.getName().equals(appVersion)) {
						context.getExtendedState().getVariables().put(VAR_APP_VERSION, appVersion);
					}
				}
			}
		}
	}

	/**
	 * {@link Action} which pushes application version into hdfs found
	 * from variable {@code appVersion}.
	 */
	protected class PushAppAction implements Action<String, String> {

		private final CloudAppType cloudAppType;

		public PushAppAction(CloudAppType cloudAppType) {
			this.cloudAppType = cloudAppType;
		}

		@Override
		public void execute(StateContext<String, String> context) {
			String appVersion = (String) context.getMessageHeader(HEADER_APP_VERSION);
			yarnCloudAppService.pushApplication(appVersion, cloudAppType);
		}
	}

	/**
	 * {@link Action} which pushes artifact into hdfs.
	 */
	protected class PushArtifactAction implements Action<String, String> {

		@Override
		public void execute(StateContext<String, String> context) {
			Resource artifact = (Resource) context.getMessageHeader(HEADER_ARTIFACT);
			String artifactDir = (String) context.getMessageHeader(HEADER_ARTIFACT_DIR);
			if (!isHdfsResource(artifact)) {
				yarnCloudAppService.pushArtifact(artifact, artifactDir);
			} else {
				if (!artifact.exists()) {
					context.getExtendedState().getVariables().put(VAR_ERROR, new RuntimeException("hdfs artifact missing"));
				}
			}
		}
	}

	/**
	 * {@link Guard} which is used to protect state where application push
	 * into hdfs would happen. Assumes that if {@code appVersion} variable
	 * exists, application is installed.
	 */
	protected class PushAppGuard implements Guard<String, String> {

		@Override
		public boolean evaluate(StateContext<String, String> context) {
			return !context.getExtendedState().getVariables().containsKey(VAR_APP_VERSION);
		}
	}

	/**
	 * {@link Guard} which is used to protect transitions happening
	 * in case on error set in variables.
	 */
	protected class ErrorGuard implements Guard<String, String> {

		@Override
		public boolean evaluate(StateContext<String, String> context) {
			return context.getExtendedState().getVariables().containsKey(VAR_ERROR);
		}
	}

	/**
	 * {@link Action} which simply logs context when machine passes
	 * through error state.
	 */
	protected class ErrorAction implements Action<String, String> {

		@Override
		public void execute(StateContext<String, String> context) {
			logger.error("Passing through error state {}", context);
		}
	}

	/**
	 * {@link Action} which wraps a delegating action and sets an error if its
	 * execution results an exception.
	 */
	private class ExceptionCatchingAction implements Action<String, String> {

		private final Action<String, String> delegate;

		public ExceptionCatchingAction(Action<String, String> delegate) {
			Assert.notNull(delegate, "Delegate action must be set");
			this.delegate = delegate;
		}

		@Override
		public void execute(StateContext<String, String> context) {
			try {
				delegate.execute(context);
			} catch (Exception e) {
				context.getExtendedState().getVariables().put(VAR_ERROR, e);
			}
		}
	}

	private boolean isHdfsResource(Resource resource) {
		try {
			return resource != null && resource.getURI().getScheme().equals("hdfs");
		} catch (IOException e) {
			return false;
		}
	}
}
