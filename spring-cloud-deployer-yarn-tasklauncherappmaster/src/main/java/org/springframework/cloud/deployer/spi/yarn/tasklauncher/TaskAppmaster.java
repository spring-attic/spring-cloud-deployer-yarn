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

package org.springframework.cloud.deployer.spi.yarn.tasklauncher;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.Lifecycle;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;
import org.springframework.yarn.am.ContainerLauncherInterceptor;
import org.springframework.yarn.am.StaticEventingAppmaster;
import org.springframework.yarn.am.container.AbstractLauncher;
import org.springframework.yarn.fs.LocalResourcesFactoryBean;
import org.springframework.yarn.fs.LocalResourcesFactoryBean.CopyEntry;
import org.springframework.yarn.fs.LocalResourcesFactoryBean.TransferEntry;
import org.springframework.yarn.fs.ResourceLocalizer;

/**
 * Spring YARN application master class which does a specific
 * handling of a task containers.
 *
 * @author Janne Valkealahti
 */
public class TaskAppmaster extends StaticEventingAppmaster {

	private final static Log log = LogFactory.getLog(TaskAppmaster.class);

	@Autowired
	private TaskAppmasterProperties taskAppmasterProperties;

	private ResourceLocalizer artifactResourceLocalizer;

	private boolean finishRequested = false;

	@Override
	protected void onInit() throws Exception {
		super.onInit();
		Assert.hasText(taskAppmasterProperties.getArtifact(), "Artifact must be set");
		artifactResourceLocalizer = buildArtifactResourceLocalizer();
		if (getLauncher() instanceof AbstractLauncher) {
			((AbstractLauncher) getLauncher()).addInterceptor(new ArtifactResourceContainerLaunchInterceptor());
		}
	}

	@Override
	protected void doStop() {
		if (getAllocator() instanceof Lifecycle) {
			((Lifecycle)getAllocator()).stop();
		}
		if (!finishRequested) {
			super.doStop();
		}
	}

	@Override
	public List<String> getCommands() {
		List<String> list = new ArrayList<String>(super.getCommands());
		if (taskAppmasterProperties.getParameters() != null) {
			for (Entry<String, String> entry : taskAppmasterProperties.getParameters().entrySet()) {
				if (StringUtils.hasText(entry.getValue())) {
					// don't pass empty values as those will most likely fail
					// with spring/boot command-line parser
					list.add(Math.max(list.size() - 2, 0), "--" + entry.getKey() + "='" + entry.getValue() + "'");
				}
			}
		}
		if (taskAppmasterProperties.getCommandlineArguments() != null) {
			for (String commandlineArgument : taskAppmasterProperties.getCommandlineArguments()) {
				list.add(Math.max(list.size() - 2, 0), "'" + commandlineArgument + "'");
			}
		}
		return list;
	}

	@Override
	protected boolean shutdownContainers() {
		// SHDP-561
		return true;
	}

	@Override
	protected void notifyCompleted() {
		// SHDP-561
		finishAppmaster();
		finishRequested = true;
		super.notifyCompleted();
	}

	private ResourceLocalizer buildArtifactResourceLocalizer() throws Exception {
		String artifact = taskAppmasterProperties.getArtifact();
		log.info("Building localizer for artifact " + artifact);
		LocalResourcesFactoryBean fb = new LocalResourcesFactoryBean();
		fb.setConfiguration(getConfiguration());
		TransferEntry te = new TransferEntry(LocalResourceType.FILE, null, artifact, false);
		ArrayList<TransferEntry> hdfsEntries = new ArrayList<TransferEntry>();
		hdfsEntries.add(te);
		fb.setHdfsEntries(hdfsEntries);
		fb.setCopyEntries(new ArrayList<CopyEntry>());
		fb.afterPropertiesSet();
		return fb.getObject();
	}

	private class ArtifactResourceContainerLaunchInterceptor implements ContainerLauncherInterceptor {

		@Override
		public ContainerLaunchContext preLaunch(Container container, ContainerLaunchContext context) {
			Map<String, LocalResource> resources = new HashMap<String, LocalResource>();
			resources.putAll(context.getLocalResources());
			resources.putAll(artifactResourceLocalizer.getResources());
			context.setLocalResources(resources);
			return context;
		}
	}
}
