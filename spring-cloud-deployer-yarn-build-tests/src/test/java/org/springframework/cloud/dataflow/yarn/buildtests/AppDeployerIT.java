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

package org.springframework.cloud.dataflow.yarn.buildtests;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.io.File;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.deployer.resource.maven.MavenProperties;
import org.springframework.cloud.deployer.resource.maven.MavenResource;
import org.springframework.cloud.deployer.spi.app.AppDeployer;
import org.springframework.cloud.deployer.spi.app.DeploymentState;
import org.springframework.cloud.deployer.spi.core.AppDefinition;
import org.springframework.cloud.deployer.spi.core.AppDeploymentRequest;
import org.springframework.cloud.deployer.spi.yarn.AppDeployerStateMachine;
import org.springframework.cloud.deployer.spi.yarn.DefaultYarnCloudAppService;
import org.springframework.cloud.deployer.spi.yarn.YarnAppDeployer;
import org.springframework.cloud.deployer.spi.yarn.YarnCloudAppService;
import org.springframework.cloud.deployer.spi.yarn.YarnDeployerProperties;
import org.springframework.cloud.deployer.spi.yarn.YarnCloudAppService.CloudAppInstanceInfo;
import org.springframework.cloud.deployer.spi.yarn.YarnCloudAppService.CloudAppType;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.core.io.Resource;
import org.springframework.core.task.TaskExecutor;
import org.springframework.data.hadoop.fs.FsShell;
import org.springframework.data.hadoop.fs.HdfsResourceLoader;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.util.FileCopyUtils;
import org.springframework.util.StringUtils;
import org.springframework.yarn.test.support.ContainerLogUtils;

/**
 * Integration tests for {@link YarnAppDeployer}.
 *
 * Tests can be run in sts if project is build first. Build
 * prepares needed yarn files in expected paths.
 * $ mvn clean package -DskipTests
 *
 * @author Janne Valkealahti
 *
 */
public class AppDeployerIT extends AbstractCliBootYarnClusterTests {

	private static final String GROUP_ID = "org.springframework.cloud.stream.module";
	private String artifactVersion;
	private AnnotationConfigApplicationContext context;

	@Before
	public void setup() {
		artifactVersion = getEnvironment().getProperty("artifactVersion");
		context = new AnnotationConfigApplicationContext();
		context.getEnvironment().setActiveProfiles("yarn");
		context.register(TestYarnConfiguration.class);
		context.setParent(getApplicationContext());
		context.refresh();
	}

	@After
	public void clean() {
		if (context != null) {
			context.close();
		}
		context = null;
	}

	@Test
	public void testStreamTimeLog() throws Exception {
		assertThat(context.containsBean("appDeployer"), is(true));
		assertThat(context.getBean("appDeployer"), instanceOf(YarnAppDeployer.class));
		AppDeployer deployer = context.getBean("appDeployer", AppDeployer.class);
		YarnCloudAppService yarnCloudAppService = context.getBean(YarnCloudAppService.class);

		MavenProperties m2Properties = new MavenProperties();
		m2Properties.setRemoteRepositories(new String[] {"https://repo.spring.io/libs-snapshot-local"});

		MavenResource timeResource = new MavenResource.Builder(m2Properties)
				.artifactId("time-source")
				.groupId(GROUP_ID)
				.version(artifactVersion)
				.extension("jar")
				.classifier("exec")
				.build();

		MavenResource logResource = new MavenResource.Builder(m2Properties)
				.artifactId("log-sink")
				.groupId(GROUP_ID)
				.version(artifactVersion)
				.extension("jar")
				.classifier("exec")
				.build();

		Map<String, String> timeProperties = new HashMap<>();
		timeProperties.put("spring.cloud.stream.bindings.output.destination", "ticktock.0");
		AppDefinition timeDefinition = new AppDefinition("time", timeProperties);
		Map<String, String> timeEnvironmentProperties = new HashMap<>();
		timeEnvironmentProperties.put(AppDeployer.COUNT_PROPERTY_KEY, "1");
		timeEnvironmentProperties.put(AppDeployer.GROUP_PROPERTY_KEY, "ticktock");
		AppDeploymentRequest timeRequest = new AppDeploymentRequest(timeDefinition, timeResource, timeEnvironmentProperties);

		Map<String, String> logProperties = new HashMap<>();
		logProperties.put("spring.cloud.stream.bindings.input.destination", "ticktock.0");
		logProperties.put("expression", "new String(payload + ' hello')");
		AppDefinition logDefinition = new AppDefinition("log", logProperties);
		Map<String, String> logEnvironmentProperties = new HashMap<>();
		logEnvironmentProperties.put(AppDeployer.COUNT_PROPERTY_KEY, "1");
		logEnvironmentProperties.put(AppDeployer.GROUP_PROPERTY_KEY, "ticktock");
		AppDeploymentRequest logRequest = new AppDeploymentRequest(logDefinition, logResource, logEnvironmentProperties);


		String timeId = deployer.deploy(timeRequest);
		assertThat(timeId, notNullValue());

		ApplicationId applicationId = assertWaitApp(2, TimeUnit.MINUTES, yarnCloudAppService);
		assertWaitFileContent(2, TimeUnit.MINUTES, applicationId, "Started TimeSourceApplication");
		assertThat(deployer.status(timeId).getState(), is(DeploymentState.deployed));

		String logId = deployer.deploy(logRequest);
		assertThat(logId, notNullValue());

		assertWaitFileContent(1, TimeUnit.MINUTES, applicationId, "Started LogSinkApplication");
		assertThat(deployer.status(logId).getState(), is(DeploymentState.deployed));

		assertWaitFileContent(1, TimeUnit.MINUTES, applicationId, "hello");

		deployer.undeploy(timeId);
		assertWaitFileContent(2, TimeUnit.MINUTES, applicationId, "stopped outbound.ticktock.0");
		deployer.undeploy(logId);
		assertWaitFileContent(2, TimeUnit.MINUTES, applicationId, "stopped inbound.ticktock.0");

		assertThat(deployer.status(timeId).getState(), is(DeploymentState.unknown));
		assertThat(deployer.status(logId).getState(), is(DeploymentState.unknown));

		List<Resource> resources = ContainerLogUtils.queryContainerLogs(
				getYarnCluster(), applicationId);

		assertThat(resources, notNullValue());
		assertThat(resources.size(), is(6));

		for (Resource res : resources) {
			File file = res.getFile();
			String content = ContainerLogUtils.getFileContent(file);
			if (file.getName().endsWith("stdout")) {
				assertThat(file.length(), greaterThan(0l));
			} else if (file.getName().endsWith("Container.stderr")) {
				assertThat("stderr with content: " + content, file.length(), is(0l));
			}
		}
	}

	@Test
	public void testStreamTimeLogAsHdfsResource() throws Exception {
		assertThat(context.containsBean("appDeployer"), is(true));
		assertThat(context.getBean("appDeployer"), instanceOf(YarnAppDeployer.class));
		AppDeployer deployer = context.getBean("appDeployer", AppDeployer.class);
		YarnCloudAppService yarnCloudAppService = context.getBean(YarnCloudAppService.class);

		MavenProperties m2Properties = new MavenProperties();
		m2Properties.setRemoteRepositories(new String[] {"https://repo.spring.io/libs-snapshot-local"});

		MavenResource timeResourceBase = new MavenResource.Builder(m2Properties)
				.artifactId("time-source")
				.groupId(GROUP_ID)
				.version(artifactVersion)
				.extension("jar")
				.classifier("exec")
				.build();

		MavenResource logResourceBase = new MavenResource.Builder(m2Properties)
				.artifactId("log-sink")
				.groupId(GROUP_ID)
				.version(artifactVersion)
				.extension("jar")
				.classifier("exec")
				.build();

		copyFile(timeResourceBase, "/dataflow/artifacts/repo/");
		copyFile(logResourceBase, "/dataflow/artifacts/repo/");

		@SuppressWarnings("resource")
		HdfsResourceLoader resourceLoader = new HdfsResourceLoader(getConfiguration());
		resourceLoader.setHandleNoprefix(true);
		Resource timeResource = resourceLoader.getResource("hdfs:/dataflow/artifacts/repo/time-source-1.0.0.BUILD-SNAPSHOT-exec.jar");
		Resource logResource = resourceLoader.getResource("hdfs:/dataflow/artifacts/repo/log-sink-1.0.0.BUILD-SNAPSHOT-exec.jar");

		Map<String, String> timeProperties = new HashMap<>();
		timeProperties.put("spring.cloud.stream.bindings.output.destination", "ticktock.0");
		AppDefinition timeDefinition = new AppDefinition("time", timeProperties);
		Map<String, String> timeEnvironmentProperties = new HashMap<>();
		timeEnvironmentProperties.put(AppDeployer.COUNT_PROPERTY_KEY, "1");
		timeEnvironmentProperties.put(AppDeployer.GROUP_PROPERTY_KEY, "ticktock");
		AppDeploymentRequest timeRequest = new AppDeploymentRequest(timeDefinition, timeResource, timeEnvironmentProperties);

		Map<String, String> logProperties = new HashMap<>();
		logProperties.put("spring.cloud.stream.bindings.input.destination", "ticktock.0");
		logProperties.put("expression", "new String(payload + ' hello')");
		AppDefinition logDefinition = new AppDefinition("log", logProperties);
		Map<String, String> logEnvironmentProperties = new HashMap<>();
		logEnvironmentProperties.put(AppDeployer.COUNT_PROPERTY_KEY, "1");
		logEnvironmentProperties.put(AppDeployer.GROUP_PROPERTY_KEY, "ticktock");
		AppDeploymentRequest logRequest = new AppDeploymentRequest(logDefinition, logResource, logEnvironmentProperties);


		String timeId = deployer.deploy(timeRequest);
		assertThat(timeId, notNullValue());

		ApplicationId applicationId = assertWaitApp(2, TimeUnit.MINUTES, yarnCloudAppService);
		assertWaitFileContent(2, TimeUnit.MINUTES, applicationId, "Started TimeSourceApplication");
		assertThat(deployer.status(timeId).getState(), is(DeploymentState.deployed));

		String logId = deployer.deploy(logRequest);
		assertThat(logId, notNullValue());

		assertWaitFileContent(1, TimeUnit.MINUTES, applicationId, "Started LogSinkApplication");
		assertThat(deployer.status(logId).getState(), is(DeploymentState.deployed));

		assertWaitFileContent(1, TimeUnit.MINUTES, applicationId, "hello");

		deployer.undeploy(timeId);
		assertWaitFileContent(2, TimeUnit.MINUTES, applicationId, "stopped outbound.ticktock.0");
		deployer.undeploy(logId);
		assertWaitFileContent(2, TimeUnit.MINUTES, applicationId, "stopped inbound.ticktock.0");

		assertThat(deployer.status(timeId).getState(), is(DeploymentState.unknown));
		assertThat(deployer.status(logId).getState(), is(DeploymentState.unknown));

		List<Resource> resources = ContainerLogUtils.queryContainerLogs(
				getYarnCluster(), applicationId);

		assertThat(resources, notNullValue());
		assertThat(resources.size(), is(6));

		for (Resource res : resources) {
			File file = res.getFile();
			String content = ContainerLogUtils.getFileContent(file);
			if (file.getName().endsWith("stdout")) {
				assertThat(file.length(), greaterThan(0l));
			} else if (file.getName().endsWith("Container.stderr")) {
				assertThat("stderr with content: " + content, file.length(), is(0l));
			}
		}
	}

	@Test
	public void testStreamTimeHdfs() throws Exception {
		assertThat(context.containsBean("appDeployer"), is(true));
		assertThat(context.getBean("appDeployer"), instanceOf(YarnAppDeployer.class));
		AppDeployer deployer = context.getBean("appDeployer", AppDeployer.class);
		YarnCloudAppService yarnCloudAppService = context.getBean(YarnCloudAppService.class);
		String fsUri = getConfiguration().get("fs.defaultFS");

		MavenProperties m2Properties = new MavenProperties();
		m2Properties.setRemoteRepositories(new String[] {"https://repo.spring.io/libs-snapshot-local"});

		MavenResource timeResource = new MavenResource.Builder(m2Properties)
				.artifactId("time-source")
				.groupId(GROUP_ID)
				.version(artifactVersion)
				.extension("jar")
				.classifier("exec")
				.build();

		MavenResource hdfsResource = new MavenResource.Builder(m2Properties)
				.artifactId("hdfs-sink")
				.groupId(GROUP_ID)
				.version(artifactVersion)
				.extension("jar")
				.classifier("exec")
				.build();

		Map<String, String> timeProperties = new HashMap<>();
		timeProperties.put("spring.cloud.stream.bindings.output.destination", "timehdfs.0");
		AppDefinition timeDefinition = new AppDefinition("time", timeProperties);
		Map<String, String> timeEnvironmentProperties = new HashMap<>();
		timeEnvironmentProperties.put(AppDeployer.COUNT_PROPERTY_KEY, "1");
		timeEnvironmentProperties.put(AppDeployer.GROUP_PROPERTY_KEY, "timehdfs");
		AppDeploymentRequest timeRequest = new AppDeploymentRequest(timeDefinition, timeResource, timeEnvironmentProperties);

		Map<String, String> hdfsProperties = new HashMap<>();
		hdfsProperties.put("spring.cloud.stream.bindings.input.destination", "timehdfs.0");
		hdfsProperties.put("spring.hadoop.fsUri", fsUri);
		AppDefinition hdfsDefinition = new AppDefinition("log", hdfsProperties);
		Map<String, String> hdfsEnvironmentProperties = new HashMap<>();
		hdfsEnvironmentProperties.put(AppDeployer.COUNT_PROPERTY_KEY, "1");
		hdfsEnvironmentProperties.put(AppDeployer.GROUP_PROPERTY_KEY, "timehdfs");
		AppDeploymentRequest hdfsRequest = new AppDeploymentRequest(hdfsDefinition, hdfsResource, hdfsEnvironmentProperties);


		String timeId = deployer.deploy(timeRequest);
		assertThat(timeId, notNullValue());

		ApplicationId applicationId = assertWaitApp(2, TimeUnit.MINUTES, yarnCloudAppService);
		assertWaitFileContent(2, TimeUnit.MINUTES, applicationId, "Started TimeSourceApplication");
		assertThat(deployer.status(timeId).getState(), is(DeploymentState.deployed));

		String hdfsId = deployer.deploy(hdfsRequest);
		assertThat(hdfsId, notNullValue());

		assertWaitFileContent(1, TimeUnit.MINUTES, applicationId, "Started HdfsSinkApplication");
		assertThat(deployer.status(hdfsId).getState(), is(DeploymentState.deployed));

		waitHdfsFile("/tmp/hdfs-sink/data-0.txt", 2, TimeUnit.MINUTES);

		deployer.undeploy(timeId);
		assertWaitFileContent(2, TimeUnit.MINUTES, applicationId, "stopped outbound.timehdfs.0");
		deployer.undeploy(hdfsId);
		assertWaitFileContent(2, TimeUnit.MINUTES, applicationId, "stopped inbound.timehdfs.0");

		List<Resource> resources = ContainerLogUtils.queryContainerLogs(
				getYarnCluster(), applicationId);

		assertThat(resources, notNullValue());
		assertThat(resources.size(), is(6));
	}

	@Test
	public void testSmokeDeployer() throws Exception {
		assertThat(context.containsBean("appDeployer"), is(true));
		assertThat(context.getBean("appDeployer"), instanceOf(YarnAppDeployer.class));
		AppDeployer deployer = context.getBean("appDeployer", AppDeployer.class);
		YarnCloudAppService yarnCloudAppService = context.getBean(YarnCloudAppService.class);

		MavenProperties m2Properties = new MavenProperties();
		m2Properties.setRemoteRepositories(new String[] {"https://repo.spring.io/libs-snapshot-local"});

		MavenResource timeResource = new MavenResource.Builder(m2Properties)
				.artifactId("time-source")
				.groupId(GROUP_ID)
				.version(artifactVersion)
				.extension("jar")
				.classifier("exec")
				.build();

		MavenResource logResource = new MavenResource.Builder(m2Properties)
				.artifactId("log-sink")
				.groupId(GROUP_ID)
				.version(artifactVersion)
				.extension("jar")
				.classifier("exec")
				.build();

		Map<String, String> timeProperties = new HashMap<>();
		timeProperties.put("spring.cloud.stream.bindings.output.destination", "ticktock.0");
		AppDefinition timeDefinition = new AppDefinition("time", timeProperties);
		Map<String, String> timeEnvironmentProperties = new HashMap<>();
		timeEnvironmentProperties.put(AppDeployer.COUNT_PROPERTY_KEY, "1");
		timeEnvironmentProperties.put(AppDeployer.GROUP_PROPERTY_KEY, "ticktock");
		AppDeploymentRequest timeRequest = new AppDeploymentRequest(timeDefinition, timeResource, timeEnvironmentProperties);

		Map<String, String> logProperties = new HashMap<>();
		logProperties.put("spring.cloud.stream.bindings.input.destination", "ticktock.0");
		logProperties.put("expression", "new String(payload + ' hello')");
		AppDefinition logDefinition = new AppDefinition("log", logProperties);
		Map<String, String> logEnvironmentProperties = new HashMap<>();
		logEnvironmentProperties.put(AppDeployer.COUNT_PROPERTY_KEY, "1");
		logEnvironmentProperties.put(AppDeployer.GROUP_PROPERTY_KEY, "ticktock");
		AppDeploymentRequest logRequest = new AppDeploymentRequest(logDefinition, logResource, logEnvironmentProperties);


		String timeId = deployer.deploy(timeRequest);
		assertThat(timeId, notNullValue());
		String logId = deployer.deploy(logRequest);
		assertThat(logId, notNullValue());

		ApplicationId applicationId = assertWaitApp(2, TimeUnit.MINUTES, yarnCloudAppService);
		assertWaitFileContent(2, TimeUnit.MINUTES, applicationId, "Started TimeSourceApplication");
		assertThat(deployer.status(timeId).getState(), is(DeploymentState.deployed));
		assertWaitFileContent(1, TimeUnit.MINUTES, applicationId, "Started LogSinkApplication");
		assertThat(deployer.status(logId).getState(), is(DeploymentState.deployed));
		assertWaitFileContent(1, TimeUnit.MINUTES, applicationId, "hello");

		for (int i = 0; i < 10; i++) {
			deployer.status(timeId);
			deployer.status(logId);
		}
		deployer.undeploy(timeId);
		for (int i = 0; i < 500; i++) {
			deployer.status(timeId);
			deployer.status(logId);
		}
		deployer.undeploy(logId);
		for (int i = 0; i < 500; i++) {
			deployer.status(timeId);
			deployer.status(logId);
		}

		assertWaitFileContent(2, TimeUnit.MINUTES, applicationId, "stopped outbound.ticktock.0");
		assertWaitFileContent(2, TimeUnit.MINUTES, applicationId, "stopped inbound.ticktock.0");

		List<Resource> resources = ContainerLogUtils.queryContainerLogs(
				getYarnCluster(), applicationId);

		assertThat(resources, notNullValue());
		assertThat(resources.size(), is(6));

		for (Resource res : resources) {
			File file = res.getFile();
			String content = ContainerLogUtils.getFileContent(file);
			if (file.getName().endsWith("stdout")) {
				assertThat(file.length(), greaterThan(0l));
			} else if (file.getName().endsWith("Container.stderr")) {
				assertThat("stderr with content: " + content, file.length(), is(0l));
			}
		}
	}

	private void copyFile(Resource artifact, String dir) throws Exception {
		File tmp = File.createTempFile(UUID.randomUUID().toString(), null);
		FileCopyUtils.copy(artifact.getInputStream(), new FileOutputStream(tmp));
		@SuppressWarnings("resource")
		FsShell shell = new FsShell(getConfiguration());
		String artifactPath = dir + artifact.getFile().getName();
		if (!shell.test(artifactPath)) {
			shell.copyFromLocal(tmp.getAbsolutePath(), dir + artifact.getFile().getName());
		}
	}

	private ApplicationId assertWaitApp(long timeout, TimeUnit unit, YarnCloudAppService yarnCloudAppService) throws Exception {
		ApplicationId applicationId = null;
		Collection<CloudAppInstanceInfo> instances;
		long end = System.currentTimeMillis() + unit.toMillis(timeout);

		do {
			instances = yarnCloudAppService.getInstances(CloudAppType.STREAM);
			if (instances.size() == 1) {
				CloudAppInstanceInfo cloudAppInstanceInfo = instances.iterator().next();
				if (StringUtils.hasText(cloudAppInstanceInfo.getAddress())) {
					applicationId = ConverterUtils.toApplicationId(cloudAppInstanceInfo.getApplicationId());
					break;
				}
			}
			Thread.sleep(1000);
		} while (System.currentTimeMillis() < end);

		assertThat(applicationId, notNullValue());
		return applicationId;
	}

	@Configuration
	@EnableConfigurationProperties({ YarnDeployerProperties.class })
	public static class TestYarnConfiguration {

		@Autowired
		private org.apache.hadoop.conf.Configuration configuration;

		@Autowired
		private Environment environment;

		@Bean
		public TaskExecutor appDeployerTaskExecutor() {
			ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
			executor.setCorePoolSize(1);
			return executor;
		}

		@Bean
		public AppDeployerStateMachine appDeployerStateMachine(YarnCloudAppService yarnCloudAppService,
				TaskExecutor appDeployer, BeanFactory beanFactory, ApplicationContext applicationContext) throws Exception {
			return new AppDeployerStateMachine(yarnCloudAppService, appDeployer, beanFactory, applicationContext);
		}

		@Bean
		public AppDeployer appDeployer(YarnCloudAppService yarnCloudAppService,
				AppDeployerStateMachine appDeployerStateMachine) throws Exception {
			return new YarnAppDeployer(yarnCloudAppService, appDeployerStateMachine.buildStateMachine());
		}

		@Bean
		public YarnCloudAppService yarnCloudAppService() {
			ApplicationContextInitializer<?>[] initializers = new ApplicationContextInitializer<?>[] {
					new HadoopConfigurationInjectingInitializer(configuration) };
			String dataflowVersion = environment.getProperty("projectVersion");
			return new DefaultYarnCloudAppService(dataflowVersion, initializers) {
				@Override
				protected List<String> processContextRunArgs(List<String> contextRunArgs) {
					List<String> newArgs = new ArrayList<>();
					if (contextRunArgs != null) {
						newArgs.addAll(contextRunArgs);
					}
					newArgs.add("--deployer.yarn.app.appmaster.path=target/spring-cloud-deployer-yarn-build-tests");
					return newArgs;
				}
			};

		}
	}
}
