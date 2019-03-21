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

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.springframework.yarn.client.ClientRmOperations;
import org.springframework.yarn.client.DefaultApplicationYarnClient;
import org.springframework.yarn.client.YarnClient;

/**
 * Custom {@link YarnClient} handling deployer specific client operations.
 *
 * @author Janne Valkealahti
 *
 */
public class DeployerApplicationYarnClient extends DefaultApplicationYarnClient {

	/**
	 * Instantiates a new deployer application yarn client.
	 *
	 * @param clientRmOperations the client rm operations
	 */
	public DeployerApplicationYarnClient(ClientRmOperations clientRmOperations) {
		super(clientRmOperations);
	}

	@Override
	protected ApplicationSubmissionContext getSubmissionContext(ApplicationId applicationId) {
		ApplicationSubmissionContext submissionContext = super.getSubmissionContext(applicationId);
		// force app fail after first appmaster failure regardles
		// what's set on yarn level.
		// TODO: should make configurable in spring yarn if possible
		submissionContext.setMaxAppAttempts(1);
		return submissionContext;
	}
}
