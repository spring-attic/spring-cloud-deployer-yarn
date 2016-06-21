/*
 * Copyright 2015-2016 the original author or authors.
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

package org.springframework.cloud.deployer.spi.yarn;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.deployer.spi.core.AppDefinition;
import org.springframework.cloud.deployer.spi.core.AppDeploymentRequest;
import org.springframework.cloud.deployer.spi.task.LaunchState;
import org.springframework.cloud.deployer.spi.task.TaskLauncher;
import org.springframework.cloud.deployer.spi.task.TaskStatus;
import org.springframework.cloud.deployer.spi.yarn.YarnCloudAppService.CloudAppInstanceInfo;
import org.springframework.cloud.deployer.spi.yarn.YarnCloudAppService.CloudAppType;
import org.springframework.core.io.Resource;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.statemachine.StateContext;
import org.springframework.statemachine.StateContext.Stage;
import org.springframework.statemachine.StateMachine;
import org.springframework.statemachine.listener.StateMachineListener;
import org.springframework.statemachine.listener.StateMachineListenerAdapter;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.util.concurrent.SettableListenableFuture;

/**
 * {@link TaskLauncher} which is launching tasks as Yarn applications.
 *
 * @author Janne Valkealahti
 */
public class YarnTaskLauncher implements TaskLauncher {

	private static final Logger logger = LoggerFactory.getLogger(YarnTaskLauncher.class);
	private final YarnCloudAppService yarnCloudAppService;
	private final StateMachine<String, String> stateMachine;

	@Autowired
	private YarnDeployerProperties yarnDeployerProperties;

	/**
	 * Instantiates a new yarn task module deployer.
	 *
	 * @param yarnCloudAppService the yarn cloud app service
	 * @param stateMachine the state machine
	 */
	public YarnTaskLauncher(YarnCloudAppService yarnCloudAppService, StateMachine<String, String> stateMachine) {
		this.yarnCloudAppService = yarnCloudAppService;
		this.stateMachine = stateMachine;
	}

	@Override
	public String launch(AppDeploymentRequest request) {
		logger.info("Deploy request for {}", request);
		logger.info("Deploy request env properties {}", request.getEnvironmentProperties());
		logger.info("Deploy definition {}", request.getDefinition());
		Resource resource = request.getResource();
		AppDefinition definition = request.getDefinition();

		String artifact = resource.getFilename();

		final String name = definition.getName();
		Map<String, String> definitionParameters = definition.getProperties();
		Map<String, String> environmentProperties = request.getEnvironmentProperties();
		List<String> commandlineArguments = request.getCommandlineArguments();
		String appName = "scdtask:" + name;

		// contextRunArgs are passed to boot app ran to control yarn apps
		// we pass needed args to control module coordinates and params,
		// weird format with '--' is just straight pass to container
		ArrayList<String> contextRunArgs = new ArrayList<String>();

		contextRunArgs.add("--spring.yarn.appName=" + appName);
		for (Entry<String, String> entry : definitionParameters.entrySet()) {
			contextRunArgs.add("--spring.yarn.client.launchcontext.arguments.--spring.cloud.deployer.yarn.appmaster.parameters." + entry.getKey() + ".='" + entry.getValue() + "'");
		}

		int index = 0;
		for (String commandlineArgument : commandlineArguments) {
			contextRunArgs.add("--spring.yarn.client.launchcontext.argumentsList[" + index
					+ "]='--spring.cloud.deployer.yarn.appmaster.commandlineArguments[" + index + "]=" + commandlineArgument + "'");
			index++;
		}

		String baseDir = yarnDeployerProperties.getBaseDir();
		if (!baseDir.endsWith("/")) {
			baseDir = baseDir + "/";
		}
		String artifactPath = isHdfsResource(resource) ? getHdfsArtifactPath(resource) : baseDir + "/artifacts/cache/";

		contextRunArgs.add("--spring.yarn.client.launchcontext.arguments.--spring.yarn.appmaster.launchcontext.archiveFile=" + artifact);
		contextRunArgs.add("--spring.yarn.client.launchcontext.arguments.--spring.cloud.deployer.yarn.appmaster.artifact=" + artifactPath + artifact);

		// deployment properties override servers.yml which overrides application.yml
		for (Entry<String, String> entry : environmentProperties.entrySet()) {
			if (entry.getKey().startsWith("deployer.yarn.app.taskcontainer")) {
				contextRunArgs.add("--spring.yarn.client.launchcontext.arguments.--" + entry.getKey() + "=" + entry.getValue());
			} else if (entry.getKey().startsWith("deployer.yarn.app.taskappmaster")) {
				contextRunArgs.add("--" + entry.getKey() + "=" + entry.getValue());
			}
		}

		final Message<String> message = MessageBuilder.withPayload(TaskLauncherStateMachine.EVENT_LAUNCH)
				.setHeader(TaskLauncherStateMachine.HEADER_APP_VERSION, "app")
				.setHeader(TaskLauncherStateMachine.HEADER_ARTIFACT, resource)
				.setHeader(TaskLauncherStateMachine.HEADER_ARTIFACT_DIR, artifactPath)
				.setHeader(TaskLauncherStateMachine.HEADER_DEFINITION_PARAMETERS, definitionParameters)
				.setHeader(TaskLauncherStateMachine.HEADER_CONTEXT_RUN_ARGS, contextRunArgs)
				.build();

		// setup future, listen event from machine and finally unregister listener,
		// and set future value
		final SettableListenableFuture<String> id = new SettableListenableFuture<>();
		final StateMachineListener<String, String> listener = new StateMachineListenerAdapter<String, String>() {

			@Override
			public void stateContext(StateContext<String, String> stateContext) {
				if (stateContext.getStage() == Stage.STATE_ENTRY && stateContext.getTarget().getId().equals(TaskLauncherStateMachine.STATE_READY)) {
					if (ObjectUtils.nullSafeEquals(message.getHeaders().getId().toString(),
							stateContext.getExtendedState().get(TaskLauncherStateMachine.VAR_MESSAGE_ID, String.class))) {
						Exception exception = stateContext.getExtendedState().get(TaskLauncherStateMachine.VAR_ERROR, Exception.class);
						if (exception != null) {
							id.setException(exception);
						} else {
							String applicationId = stateContext.getStateMachine().getExtendedState()
									.get(TaskLauncherStateMachine.VAR_APPLICATION_ID, String.class);
							DeploymentKey key = new DeploymentKey(name, applicationId);
							id.set(key.toString());
						}
					}
				}
			}
		};

		stateMachine.addStateListener(listener);
		id.addCallback(new ListenableFutureCallback<String>() {

			@Override
			public void onSuccess(String result) {
				stateMachine.removeStateListener(listener);
			}

			@Override
			public void onFailure(Throwable ex) {
				stateMachine.removeStateListener(listener);
			}
		});

		stateMachine.sendEvent(message);
		// we need to block here until SPI supports
		// returning id asynchronously
		try {
			return id.get(2, TimeUnit.MINUTES);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private boolean isHdfsResource(Resource resource) {
		try {
			return resource != null && resource.getURI().getScheme().equals("hdfs");
		} catch (IOException e) {
			return false;
		}
	}

	private String getHdfsArtifactPath(Resource resource) {
		String path = null;
		try {
			path = "/" + FilenameUtils.getPath(resource.getURI().getPath());
		} catch (IOException e) {
		}
		return path;
	}

	@Override
	public void cancel(String id) {
		logger.info("Undeploy request for task {}", id);
		DeploymentKey key = new DeploymentKey(id);
		Message<String> message = MessageBuilder.withPayload(TaskLauncherStateMachine.EVENT_CANCEL)
				.setHeader(TaskLauncherStateMachine.HEADER_APPLICATION_ID, key.applicationId)
				.build();
		stateMachine.sendEvent(message);
	}

	@Override
	public TaskStatus status(String id) {
		logger.info("Status request for module {}", id);
		DeploymentKey key = new DeploymentKey(id);

		Collection<CloudAppInstanceInfo> instances = yarnCloudAppService.getInstances(CloudAppType.TASK);
		for (CloudAppInstanceInfo instance : instances) {
			if (instance.getApplicationId().equals(key.applicationId)) {
				if (instance.getState() == "RUNNING") {
					return new TaskStatus(id, LaunchState.running, null);
				}
			}
		}
		return new TaskStatus(id, LaunchState.unknown, null);
	}

	private static class DeploymentKey {
		final static String SEPARATOR = ":";
		final String name;
		final String applicationId;

		public DeploymentKey(String id) {
			String[] split = id.split(SEPARATOR);
			Assert.isTrue(split.length == 2, "Unable to parse deployment key " + id);
			name = split[0];
			applicationId = split[1];
		}

		public DeploymentKey(String name, String applicationId) {
			Assert.notNull(name, "Name must be set");
			Assert.notNull(applicationId, "Application id must be set");
			this.name = name;
			this.applicationId = applicationId;
		}

		@Override
		public String toString() {
			return name + SEPARATOR + applicationId;
		}
	}
}
