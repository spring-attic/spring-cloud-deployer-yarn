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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.junit.After;
import org.junit.Before;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.core.io.Resource;
import org.springframework.yarn.support.console.ContainerClusterReport.ClustersInfoReportData;

public class AbstractStateMachineTests {

	protected AnnotationConfigApplicationContext context;

	@Before
	public void setup() {
		context = buildContext();
	}

	@After
	public void clean() {
		if (context != null) {
			context.close();
		}
		context = null;
	}

	protected AnnotationConfigApplicationContext buildContext() {
		return null;
	}

	protected static class TestYarnCloudAppService implements YarnCloudAppService {

		volatile String app = null;
		volatile String instance = null;

		final CountDownLatch getApplicationsLatch = new CountDownLatch(1);
		final CountDownLatch getInstancesLatch = new CountDownLatch(2);
		final CountDownLatch pushApplicationLatch = new CountDownLatch(1);
		final CountDownLatch submitApplicationLatch = new CountDownLatch(1);
		final CountDownLatch createClusterLatch = new CountDownLatch(1);
		final CountDownLatch startClusterLatch = new CountDownLatch(1);
		final CountDownLatch stopClusterLatch = new CountDownLatch(1);

		volatile int getApplicationsCount = 0;
		volatile int getInstancesCount = 0;
		volatile int getInstancesCountBeforeReturn = 0;

		final List<Wrapper> pushApplicationCount = Collections.synchronizedList(new ArrayList<Wrapper>());
		final List<Wrapper> submitApplicationCount = Collections.synchronizedList(new ArrayList<Wrapper>());
		final List<Wrapper> createClusterCount = Collections.synchronizedList(new ArrayList<Wrapper>());
		final List<Wrapper> startClusterCount = Collections.synchronizedList(new ArrayList<Wrapper>());
		final List<Wrapper> stopClusterCount = Collections.synchronizedList(new ArrayList<Wrapper>());

		@Override
		public Collection<CloudAppInfo> getApplications(CloudAppType cloudAppType) {
			ArrayList<CloudAppInfo> infos = new ArrayList<CloudAppInfo>();
			if (app != null) {
				infos.add(new CloudAppInfo(app));
			}
			getApplicationsCount++;
			getApplicationsLatch.countDown();
			return infos;
		}

		@Override
		public Collection<CloudAppInstanceInfo> getInstances(CloudAppType cloudAppType) {
			ArrayList<CloudAppInstanceInfo> infos = new ArrayList<CloudAppInstanceInfo>();
			if (instance != null) {
				if (getInstancesCount >= getInstancesCountBeforeReturn) {
					infos.add(new CloudAppInstanceInfo("fakeApplicationId", instance, "RUNNING", "http://fakeAddress"));
				}
			}
			getInstancesCount++;
			getInstancesLatch.countDown();
			return infos;
		}

		@Override
		public void pushApplication(String appVersion, CloudAppType cloudAppType) {
			app = appVersion;
			pushApplicationCount.add(new Wrapper(appVersion));
			pushApplicationLatch.countDown();
		}

		@Override
		public String submitApplication(String appVersion, CloudAppType cloudAppType) {
			instance = "scdstream:" + appVersion;
			submitApplicationCount.add(new Wrapper(appVersion));
			submitApplicationLatch.countDown();
			return "fakeApplicationId";
		}

		@Override
		public String submitApplication(String appVersion, CloudAppType cloudAppType, List<String> contextRunArgs) {
			instance = "scdstream:" + appVersion + ":fakeGroup";
			submitApplicationCount.add(new Wrapper(appVersion));
			submitApplicationLatch.countDown();
			return "fakeApplicationId";
		}

		@Override
		public void killApplications(String appName, CloudAppType cloudAppType) {
		}

		@Override
		public void createCluster(String yarnApplicationId, String clusterId, int count, String artifact,
				Map<String, String> definitionParameters) {
			createClusterCount.add(new Wrapper(yarnApplicationId, clusterId, count, definitionParameters));
			createClusterLatch.countDown();
		}

		@Override
		public void startCluster(String yarnApplicationId, String clusterId) {
			startClusterCount.add(new Wrapper(yarnApplicationId, clusterId));
			startClusterLatch.countDown();
		}

		@Override
		public void stopCluster(String yarnApplicationId, String clusterId) {
			stopClusterCount.add(new Wrapper(yarnApplicationId, clusterId));
			stopClusterLatch.countDown();
		}

		@Override
		public Collection<String> getClusters(String yarnApplicationId) {
			return null;
		}

		@Override
		public void destroyCluster(String yarnApplicationId, String clusterId) {
		}

		@Override
		public void pushArtifact(Resource artifact, String dir) {
		}

		static class Wrapper {
			String appVersion;
			String yarnApplicationId;
			String clusterId;
			int count;
			Map<?, ?> definitionParameters;

			public Wrapper(String appVersion) {
				this.appVersion = appVersion;
			}

			public Wrapper(String yarnApplicationId, String clusterId) {
				this.yarnApplicationId = yarnApplicationId;
				this.clusterId = clusterId;
			}

			public Wrapper(String yarnApplicationId, String clusterId, int count,
					Map<?, ?> definitionParameters) {
				this.yarnApplicationId = yarnApplicationId;
				this.clusterId = clusterId;
				this.count = count;
				this.definitionParameters = definitionParameters;
			}

		}

		@Override
		public Map<String, StreamClustersInfoReportData> getClustersStates(String yarnApplicationId) {
			return null;
		}

		@Override
		public void killApplication(String yarnApplicationId, CloudAppType cloudAppType) {
		}

	}
}
