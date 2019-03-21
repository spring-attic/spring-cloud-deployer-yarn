/*
 * Copyright 2015-2017 the original author or authors.
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

package org.springframework.cloud.deployer.spi.yarn.appdeployer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.StringUtils;
import org.springframework.yarn.am.ContainerLauncherInterceptor;
import org.springframework.yarn.am.cluster.ContainerCluster;
import org.springframework.yarn.am.cluster.ManagedContainerClusterAppmaster;
import org.springframework.yarn.am.container.AbstractLauncher;
import org.springframework.yarn.am.grid.GridMember;
import org.springframework.yarn.am.grid.support.ProjectionData;
import org.springframework.yarn.fs.LocalResourcesFactoryBean;
import org.springframework.yarn.fs.LocalResourcesFactoryBean.CopyEntry;
import org.springframework.yarn.fs.LocalResourcesFactoryBean.TransferEntry;
import org.springframework.yarn.fs.ResourceLocalizer;
import org.springframework.yarn.listener.ContainerMonitorListener;

/**
 * Custom yarn appmaster tweaking container launch settings.
 *
 * @author Janne Valkealahti
 *
 */
public class StreamAppmaster extends ManagedContainerClusterAppmaster {

	private final static Log log = LogFactory.getLog(StreamAppmaster.class);
	private final Map<String, ResourceLocalizer> artifactLocalizers = new HashMap<>();
	private final ContainerIndexTracker indexTracker = new ContainerIndexTracker();

	/** ContainerId to ContainerCluster id map */
	private final Map<ContainerId, String> containerIdMap = new HashMap<>();

	@Autowired
	private StreamAppmasterProperties streamAppmasterProperties;

	@Override
	protected void onInit() throws Exception {
		super.onInit();

		// TODO: we want to have a proper support in base classes to gracefully
		//       shutdown appmaster when it has nothing to do. this trick
		//       here is solely a workaround not being able to access internal
		//       structures of base classes. this is pretty much all we can do
		//       from a subclass.
		//       potentially we want to make it configurable with a grace period, etc.
		getMonitor().addContainerMonitorStateListener(new ContainerMonitorListener() {

			@Override
			public void state(ContainerMonitorState state) {
				if (log.isDebugEnabled()) {
					log.info("Received monitor state " + state + " and container clusters size is " + getContainerClusters().size());
				}
				if (state.getRunning() == 0 && getContainerClusters().size() == 0) {
					// this state is valid at start but we know it's not gonna
					// get called until we have had at least one container running
					log.info("No running containers and no container clusters, initiate app shutdown");
					notifyCompleted();
				}
			}
		});

		if (getLauncher() instanceof AbstractLauncher) {
			((AbstractLauncher)getLauncher()).addInterceptor(new IndexAddingContainerLauncherInterceptor());
		}
	}

	@Override
	public ContainerCluster createContainerCluster(String clusterId, String clusterDef, ProjectionData projectionData,
			Map<String, Object> extraProperties) {
		log.info("intercept createContainerCluster " + clusterId);
		String artifactPath = streamAppmasterProperties.getArtifact();
		try {
			LocalResourcesFactoryBean lrfb = new LocalResourcesFactoryBean();
			lrfb.setConfiguration(getConfiguration());

			String containerArtifact = (String) extraProperties.get("containerArtifact");
			TransferEntry te = new TransferEntry(LocalResourceType.FILE, null, artifactPath + "/" + containerArtifact, false);
			ArrayList<TransferEntry> hdfsEntries = new ArrayList<TransferEntry>();
			hdfsEntries.add(te);
			lrfb.setHdfsEntries(hdfsEntries);
			lrfb.setCopyEntries(new ArrayList<CopyEntry>());
			lrfb.afterPropertiesSet();
			ResourceLocalizer rl = lrfb.getObject();
			log.info("Adding localizer for " + clusterId + " / " + rl);
			artifactLocalizers.put(clusterId, rl);
		} catch (Exception e) {
			log.error("Error creating localizer", e);
		}

		return super.createContainerCluster(clusterId, clusterDef, projectionData, extraProperties);
	}

	@Override
	protected List<String> onContainerLaunchCommands(Container container, ContainerCluster cluster,
			List<String> commands) {
		log.info("onContainerLaunchCommands commands=" + StringUtils.collectionToCommaDelimitedString(commands));
		ArrayList<String> list = new ArrayList<String>();
		Map<String, Object> extraProperties = cluster.getExtraProperties();
		String artifact = (String) extraProperties.get("containerArtifact");

		for (String command : commands) {
			if (command.contains("placeholder.jar")) {
				list.add(command.replace("placeholder.jar", artifact));
			} else {
				list.add(command);
			}
		}

		if (extraProperties != null) {
			for (Entry<String, Object> entry : extraProperties.entrySet()) {
				if (entry.getKey().startsWith("containerArg")) {
					log.info("onContainerLaunchCommands adding command=--" + entry.getValue().toString());
					list.add(Math.max(list.size() - 2, 0), "--" + entry.getValue().toString());
				}
			}
		}
		return list;
	}

	@Override
	protected Map<String, LocalResource> buildLocalizedResources(ContainerCluster cluster) {
		Map<String, LocalResource> resources = super.buildLocalizedResources(cluster);
		ResourceLocalizer rl = artifactLocalizers.get(cluster.getId());
		log.info("Localizer for " + cluster.getId() + " is " + rl);
		resources.putAll(rl.getResources());
		return resources;
	}

	@Override
	protected void onContainerCompleted(ContainerStatus status) {
		super.onContainerCompleted(status);
		String containerClusterId = containerIdMap.get(status.getContainerId());
		if (containerClusterId != null) {
			synchronized (indexTracker) {
				indexTracker.freeIndex(status.getContainerId(), containerClusterId);
			}
		}
	}

	private ContainerCluster findContainerClusterByContainerId(ContainerId containerId) {
		for (Entry<String, ContainerCluster> entry : getContainerClusters().entrySet()) {
			for (GridMember member : entry.getValue().getGridProjection().getMembers()) {
				if (member.getId().equals(containerId)) {
					return entry.getValue();
				}
			}
		}
		return null;
	}

	/**
	 * Interceptor adding INSTANCE_INDEX as env variable based on ContainerIndexTracker.
	 */
	private class IndexAddingContainerLauncherInterceptor implements ContainerLauncherInterceptor {

		@Override
		public ContainerLaunchContext preLaunch(Container container, ContainerLaunchContext context) {
			ContainerCluster containerCluster = findContainerClusterByContainerId(container.getId());
			if (containerCluster == null) {
				return context;
			}
			containerIdMap.put(container.getId(), containerCluster.getId());

			Map<String, String> environment = context.getEnvironment();
			Map<String, String> indexEnv = new HashMap<>();
			indexEnv.putAll(environment);
			Integer reservedIndex;
			synchronized (indexTracker) {
				reservedIndex = indexTracker.reserveIndex(container.getId(), containerCluster);
			}
			indexEnv.put("SPRING_CLOUD_APPLICATION_GUID", container.getId().toString());
			indexEnv.put("SPRING_APPLICATION_INDEX", Integer.toString(reservedIndex));
			indexEnv.put("INSTANCE_INDEX", Integer.toString(reservedIndex));
			context.setEnvironment(indexEnv);
			return context;
		}
	}

	/**
	 * Support class tracking containers per group and reserving an index sequence.
	 */
	private static class ContainerIndexTracker {
		// TODO: move this feature to spring-yarn where scaling can be
		//       implemented accurately. scaling up/down is anyway not
		//       supported in sc stream at this moment.
		Map<String, ArrayList<ContainerId>> reservationsMap = new HashMap<>();

		Integer reserveIndex(ContainerId containerId, ContainerCluster containerCluster) {
			ArrayList<ContainerId> reservationList = reservationsMap.get(containerCluster.getId());
			if (reservationList == null) {
				reservationList = new ArrayList<>();
				reservationsMap.put(containerCluster.getId(), reservationList);
			}

			// we always increment index at least once
			Iterator<ContainerId> iterator = reservationList.iterator();
			int index = -1;
			ContainerId n = null;
			while(iterator.hasNext()) {
				ContainerId nn = iterator.next();
				index++;
				if (nn == null) {
					// we found existing nullified reservation, use that
					n = containerId;
					break;
				}
			}

			// all resevations in use, add new
			if (n == null) {
				reservationList.add(containerId);
				index++;
			}
			return index;
		}

		void freeIndex(ContainerId containerId, String containerClusterId) {
			ArrayList<ContainerId> reservationList = reservationsMap.get(containerClusterId);
			if (reservationList != null) {
				for (int index = 0; index < reservationList.size(); index++) {
					if (containerId.equals(reservationList.get(index))) {
						// nullify existing reservation
						reservationList.set(index, null);
						return;
					}
				}
			}
		}
	}
}
