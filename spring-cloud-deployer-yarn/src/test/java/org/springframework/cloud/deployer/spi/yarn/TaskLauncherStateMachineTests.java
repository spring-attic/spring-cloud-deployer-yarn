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
import java.util.HashMap;

import org.junit.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.statemachine.StateMachine;
import org.springframework.statemachine.test.StateMachineTestPlan;
import org.springframework.statemachine.test.StateMachineTestPlanBuilder;

public class TaskLauncherStateMachineTests extends AbstractStateMachineTests {

	@Test
	public void testInitial() throws Exception {
		context.register(Config.class);
		context.refresh();

		TestYarnCloudAppService yarnCloudAppService = new TestYarnCloudAppService();
		TaskExecutor taskExecutor = context.getBean(TaskExecutor.class);
		TaskLauncherStateMachine ycasm = new TaskLauncherStateMachine(yarnCloudAppService, taskExecutor, context, context);
		ycasm.setAutoStart(false);
		StateMachine<String, String> stateMachine = ycasm.buildStateMachine();

		StateMachineTestPlan<String, String> plan =
				StateMachineTestPlanBuilder.<String, String>builder()
					.defaultAwaitTime(10)
					.stateMachine(stateMachine)
					.step()
						.expectStateMachineStarted(1)
						.expectStates(TaskLauncherStateMachine.STATE_READY)
						.and()
					.build();
		plan.test();
	}

	@Test
	public void testLaunchShouldPushAndStart() throws Exception {
		context.register(Config.class);
		context.refresh();

		TestYarnCloudAppService yarnCloudAppService = new TestYarnCloudAppService();
		TaskExecutor taskExecutor = context.getBean(TaskExecutor.class);
		TaskLauncherStateMachine ycasm = new TaskLauncherStateMachine(yarnCloudAppService, taskExecutor, context, context);
		ycasm.setAutoStart(false);
		StateMachine<String, String> stateMachine = ycasm.buildStateMachine();

		ArrayList<String> contextRunArgs = new ArrayList<String>();
		Message<String> launchMessage = MessageBuilder.withPayload(TaskLauncherStateMachine.EVENT_LAUNCH)
				.setHeader(TaskLauncherStateMachine.HEADER_APP_VERSION, "fakeApp")
				.setHeader(TaskLauncherStateMachine.HEADER_DEFINITION_PARAMETERS, new HashMap<Object, Object>())
				.setHeader(TaskLauncherStateMachine.HEADER_CONTEXT_RUN_ARGS, contextRunArgs)
				.build();

		StateMachineTestPlan<String, String> plan =
				StateMachineTestPlanBuilder.<String, String>builder()
					.defaultAwaitTime(10)
					.stateMachine(stateMachine)
					.step()
						.expectStateMachineStarted(1)
						.expectStates(TaskLauncherStateMachine.STATE_READY)
						.and()
					.step()
						.sendEvent(launchMessage)
						.expectStateChanged(6)
						.expectStates(TaskLauncherStateMachine.STATE_READY)
						.and()
					.build();
		plan.test();
	}

	@Configuration
	static class Config {

		@Bean
		TaskExecutor taskExecutor() {
			ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
			taskExecutor.setCorePoolSize(1);
			return taskExecutor;
		}

	}

	@Override
	protected AnnotationConfigApplicationContext buildContext() {
		return new AnnotationConfigApplicationContext();
	}
}
