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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.util.Collector;

import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableSet;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Log parser for converting the JSON strings into {@link Map}s. We use Jackson to parse the jsons,
 * then we convert each field to String for easier indexing.
 * <p>
 * We remove all unnecessary fields before indexing and extract the yarn application id from the
 * container id if it's present in the log.
 */
public class LogParser implements FlatMapFunction<String, Map<String, String>>, ResultTypeQueryable<Map<String, String>> {

	public static Logger LOG = LoggerFactory.getLogger(LogParser.class);

	public static final String YARN_CONTAINER_ID_KEY = "yarnContainerId";
	public static final String YARN_APP_ID_KEY = "yarnApplicationId";

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	public static final Set<String> removedFields = ImmutableSet.of("mdc", "file", "@version");

	@Override
	public void flatMap(String logLine, Collector<Map<String, String>> out) throws Exception {
		try {
			Map<String, ?> jsonRaw = OBJECT_MAPPER.readValue(logLine, Map.class);

			Map<String, String> stringMap = jsonRaw.entrySet().stream()
					.filter(e -> !removedFields.contains(e.getKey()))
					.collect(Collectors.toMap(Map.Entry::getKey, e -> String.valueOf(e.getValue())));

			Optional<String> containerId = Optional.ofNullable(stringMap.get(YARN_CONTAINER_ID_KEY));
			Optional<String> appId = containerId.map(cId -> {
				String[] split = cId.split("_");
				return String.join("_", "application", split[1], split[2]);
			});

			stringMap.put(YARN_CONTAINER_ID_KEY, containerId.orElse("UNKNOWN"));
			stringMap.put(YARN_APP_ID_KEY, appId.orElse("UNKNOWN"));
			out.collect(stringMap);
		} catch (Throwable t) {
			LOG.error("Could not parse: " + logLine, t);
		}
	}

	@Override
	public TypeInformation<Map<String, String>> getProducedType() {
		return new MapTypeInfo<>(String.class, String.class);
	}
}