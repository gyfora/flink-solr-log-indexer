/*
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera.flink.solr;

import org.apache.flink.util.Collector;

import org.apache.commons.io.FileUtils;
import org.junit.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class LogParserTest {

	@Test
	public void parsingTest() throws Exception {
		String logLine = FileUtils.readFileToString(new File("src/test/resources/log1.json"), StandardCharsets.UTF_8);

		TestingCollector<Map<String, String>> collector = new TestingCollector<Map<String, String>>();

		LogParser logParser = new LogParser();
		logParser.flatMap(logLine, collector);

		assertEquals("gyula-2.gce.cloudera.com", collector.last.get("source_host"));
		assertEquals("2019-10-14T11:21:07.400Z", collector.last.get("@timestamp"));
		for (String key : LogParser.removedFields) {
			assertFalse(collector.last.containsKey(key));
		}
		assertTrue(collector.last.containsKey(LogParser.YARN_CONTAINER_ID_KEY));
		assertTrue(collector.last.containsKey(LogParser.YARN_APP_ID_KEY));

		logLine = FileUtils.readFileToString(new File("src/test/resources/log2.json"), StandardCharsets.UTF_8);

		logParser.flatMap(logLine, collector);
		assertEquals("UNKNOWN", collector.last.get(LogParser.YARN_CONTAINER_ID_KEY));
		assertEquals("UNKNOWN", collector.last.get(LogParser.YARN_APP_ID_KEY));
	}

	private static class TestingCollector<T> implements Collector<T> {

		T last;

		@Override
		public void collect(T t) {
			last = t;
		}

		@Override
		public void close() {
		}
	}
}
