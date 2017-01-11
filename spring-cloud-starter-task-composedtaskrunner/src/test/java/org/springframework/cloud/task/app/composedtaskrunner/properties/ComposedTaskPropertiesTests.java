/*
 *
 * Copyright 2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.task.app.composedtaskrunner.properties;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author Glenn Renfro
 */
public class ComposedTaskPropertiesTests {

	@Test
	public void testGettersAndSetters() {
		ComposedTaskProperties properties = new ComposedTaskProperties();
		properties.setComposedTaskProperties("aaa");
		properties.setComposedTaskArguments("bbb");
		properties.setIntervalTimeBetweenChecks(12345);
		properties.setMaxWaitTime(6789);
		properties.setDataFlowUri("ccc");
		properties.setGraph("ddd");
		properties.setJobNamePrefix("eee");
		assertEquals("aaa", properties.getComposedTaskProperties());
		assertEquals("bbb", properties.getComposedTaskArguments());
		assertEquals(12345, properties.getIntervalTimeBetweenChecks());
		assertEquals(6789, properties.getMaxWaitTime());
		assertEquals("ccc", properties.getDataFlowUri());
		assertEquals("ddd", properties.getGraph());
		assertEquals("eee", properties.getJobNamePrefix());
	}
}
