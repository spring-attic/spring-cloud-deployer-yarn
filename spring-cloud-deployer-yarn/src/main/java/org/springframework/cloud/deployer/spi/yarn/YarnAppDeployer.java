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
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.deployer.spi.app.AppDeployer;
import org.springframework.cloud.deployer.spi.app.AppInstanceStatus;
import org.springframework.cloud.deployer.spi.app.AppStatus;
import org.springframework.cloud.deployer.spi.app.AppStatus.Builder;
import org.springframework.cloud.deployer.spi.app.DeploymentState;
import org.springframework.cloud.deployer.spi.core.AppDefinition;
import org.springframework.cloud.deployer.spi.core.AppDeploymentRequest;
import org.springframework.core.io.Resource;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.statemachine.StateContext;
import org.springframework.statemachine.StateMachine;
import org.springframework.statemachine.StateContext.Stage;
import org.springframework.statemachine.listener.StateMachineListener;
import org.springframework.statemachine.listener.StateMachineListenerAdapter;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.util.concurrent.SettableListenableFuture;
import org.springframework.yarn.support.console.ContainerClusterReport.ClustersInfoReportData;

/**
 * {@link ModuleDeployer} which communicates with a Yarn app running
 * on a Hadoop cluster waiting for deployment requests. This app
 * uses Spring Yarn's container grouping functionality to create a
 * new group per module type. This allows all modules to share the
 * same settings and the group itself can controlled, i.e. ramp up/down
 * or shutdown/destroy a whole group.
 *
 * @author Janne Valkealahti
 */
public class YarnAppDeployer implements AppDeployer {

	private static final Logger logger = LoggerFactory.getLogger(YarnAppDeployer.class);
	private final YarnCloudAppService yarnCloudAppService;
	private final StateMachine<String, String> stateMachine;

	@Autowired
	private YarnDeployerProperties yarnDeployerProperties;

	/**
	 * Instantiates a new yarn stream module deployer.
	 *
	 * @param yarnCloudAppService the yarn cloud app service
	 * @param stateMachine the state machine
	 */
	public YarnAppDeployer(YarnCloudAppService yarnCloudAppService, StateMachine<String, String> stateMachine) {
		this.yarnCloudAppService = yarnCloudAppService;
		this.stateMachine = stateMachine;
	}

	@Override
	public String deploy(AppDeploymentRequest request) {
		logger.info("Deploy request for {}", request);
		final AppDefinition definition = request.getDefinition();
		Map<String, String> definitionParameters = definition.getProperties();
		Map<String, String> environmentProperties = request.getEnvironmentProperties();
		logger.info("Deploying request for definition {}", definition);
		logger.info("Parameters for definition {}", definitionParameters);
		logger.info("Environment properties for request {}", environmentProperties);

		int count = 1;
		String countString = request.getEnvironmentProperties().get(AppDeployer.COUNT_PROPERTY_KEY);
		if (StringUtils.hasText(countString)) {
			count = Integer.parseInt(countString);
		}
		final String group = request.getEnvironmentProperties().get(AppDeployer.GROUP_PROPERTY_KEY);
		Resource resource = request.getResource();
		final String clusterId = group + ":" + definition.getName();

		// contextRunArgs are passed to boot app ran to control yarn apps
		ArrayList<String> contextRunArgs = new ArrayList<String>();
		contextRunArgs.add("--spring.yarn.appName=scdstream:app:" + group);

		// deployment properties override servers.yml which overrides application.yml
		for (Entry<String, String> entry : environmentProperties.entrySet()) {
			if (entry.getKey().startsWith("deployer.yarn.app.streamappmaster")) {
				contextRunArgs.add("--" + entry.getKey() + "=" + entry.getValue());
			} else if (entry.getKey().startsWith("deployer.yarn.app.streamcontainer")) {
				// weird format with '--' is just straight pass to appmaster
				contextRunArgs.add("--spring.yarn.client.launchcontext.arguments.--" + entry.getKey() + "=" + entry.getValue());
			}
		}

		String baseDir = yarnDeployerProperties.getBaseDir();
		if (!baseDir.endsWith("/")) {
			baseDir = baseDir + "/";
		}

		String artifactPath = isHdfsResource(resource) ? getHdfsArtifactPath(resource) : baseDir + "/artifacts/cache/";
		contextRunArgs.add("--spring.yarn.client.launchcontext.arguments.--spring.cloud.deployer.yarn.appmaster.artifact=" + artifactPath);

		// TODO: using default app name "app" until we start to customise
		//       via deploymentProperties
		final Message<String> message = MessageBuilder.withPayload(AppDeployerStateMachine.EVENT_DEPLOY)
				.setHeader(AppDeployerStateMachine.HEADER_APP_VERSION, "app")
				.setHeader(AppDeployerStateMachine.HEADER_CLUSTER_ID, clusterId)
				.setHeader(AppDeployerStateMachine.HEADER_GROUP_ID, group)
				.setHeader(AppDeployerStateMachine.HEADER_COUNT, count)
				.setHeader(AppDeployerStateMachine.HEADER_ARTIFACT, resource)
				.setHeader(AppDeployerStateMachine.HEADER_ARTIFACT_DIR, artifactPath)
				.setHeader(AppDeployerStateMachine.HEADER_DEFINITION_PARAMETERS, definitionParameters)
				.setHeader(AppDeployerStateMachine.HEADER_CONTEXT_RUN_ARGS, contextRunArgs)
				.build();

		// Use of future here is to set id when it becomes available from machine
		final SettableListenableFuture<String> id = new SettableListenableFuture<>();
		final StateMachineListener<String, String> listener = new StateMachineListenerAdapter<String, String>() {

			@Override
			public void stateContext(StateContext<String, String> stateContext) {
				if (stateContext.getStage() == Stage.STATE_ENTRY && stateContext.getTarget().getId().equals(AppDeployerStateMachine.STATE_READY)) {
					if (ObjectUtils.nullSafeEquals(message.getHeaders().getId().toString(),
							stateContext.getExtendedState().get(AppDeployerStateMachine.VAR_MESSAGE_ID, String.class))) {
						Exception exception = stateContext.getExtendedState().get(AppDeployerStateMachine.VAR_ERROR, Exception.class);
						if (exception != null) {
							id.setException(exception);
						} else {
							String applicationId = stateContext.getStateMachine().getExtendedState()
									.get(AppDeployerStateMachine.VAR_APPLICATION_ID, String.class);
							DeploymentKey key = new DeploymentKey(group, definition.getName(), applicationId);
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

	@Override
	public void undeploy(String id) {
		logger.info("Undeploy request for id {}", id);
		DeploymentKey key = new DeploymentKey(id);
		Message<String> message = MessageBuilder.withPayload(AppDeployerStateMachine.EVENT_UNDEPLOY)
				.setHeader(AppDeployerStateMachine.HEADER_CLUSTER_ID, key.getClusterId())
				.setHeader(AppDeployerStateMachine.HEADER_APP_VERSION, "app")
				.setHeader(AppDeployerStateMachine.HEADER_GROUP_ID, key.group)
				.setHeader(AppDeployerStateMachine.HEADER_APPLICATION_ID, key.applicationId)
				.build();
		stateMachine.sendEvent(message);
	}

	@Override
	public AppStatus status(String id) {
		logger.info("Checking status of {}", id);
		DeploymentKey key = new DeploymentKey(id);
		Builder builder = AppStatus.of(id);
		for (Entry<String, ClustersInfoReportData> entry : yarnCloudAppService.getClustersStates(key.applicationId).entrySet()) {
			if (ObjectUtils.nullSafeEquals(entry.getKey(), key.getClusterId())) {
				ClustersInfoReportData data = entry.getValue();
				for (int i = 0 ; i < data.getProjectionAny(); i++) {
					InstanceStatus instanceStatus = new InstanceStatus(key.getClusterId() + "-" + i, i < data.getCount(), null);
					builder.with(instanceStatus);
				}
				break;
			}
		}
		return builder.build();
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

	private static class DeploymentKey {
		final static String SEPARATOR = ":";
		final String group;
		final String name;
		final String applicationId;
		final String clusterId;

		public DeploymentKey(String id) {
			String[] split = id.split(SEPARATOR);
			Assert.isTrue(split.length == 3, "Unable to parse deployment key " + id);
			group = split[0];
			name = split[1];
			applicationId = split[2];
			clusterId = group + SEPARATOR + name;
		}

		public DeploymentKey(String group, String name, String applicationId) {
			Assert.notNull(group, "Group must be set");
			Assert.notNull(name, "Name must be set");
			Assert.notNull(applicationId, "Application id must be set");
			this.group = group;
			this.name = name;
			this.applicationId = applicationId;
			clusterId = group + SEPARATOR + name;
		}

		public String getClusterId() {
			return clusterId;
		}

		@Override
		public String toString() {
			return group + SEPARATOR + name + SEPARATOR + applicationId;
		}
	}

	private static class InstanceStatus implements AppInstanceStatus {
		private final String id;
		private final DeploymentState state;
		private final Map<String, String> attributes = new HashMap<String, String>();

		public InstanceStatus(String id, boolean deployed, Map<String, String> attributes) {
			this.id = id;
			this.state = deployed ? DeploymentState.deployed : DeploymentState.unknown;
			if (attributes != null) {
				this.attributes.putAll(attributes);
			}
		}

		@Override
		public String getId() {
			return id;
		}

		@Override
		public DeploymentState getState() {
			return state;
		}

		@Override
		public Map<String, String> getAttributes() {
			return attributes;
		}

		@Override
		public String toString() {
			return "InstanceStatus [id=" + id + ", state=" + state + ", attributes=" + attributes + "]";
		}
	}
}
