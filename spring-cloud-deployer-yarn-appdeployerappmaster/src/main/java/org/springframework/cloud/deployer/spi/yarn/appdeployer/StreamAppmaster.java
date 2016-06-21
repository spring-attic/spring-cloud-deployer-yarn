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

package org.springframework.cloud.deployer.spi.yarn.appdeployer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.yarn.am.cluster.ContainerCluster;
import org.springframework.yarn.am.cluster.ManagedContainerClusterAppmaster;
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
}
