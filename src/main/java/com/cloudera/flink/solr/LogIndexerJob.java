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

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import org.apache.solr.client.solrj.response.UpdateResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Main application entrypoint. Creates and executes the log indexer job.
 */
public class LogIndexerJob {

	public static final Logger LOG = LoggerFactory.getLogger(LogIndexerJob.class);

	public static final String LOG_INPUT_TOPIC_KEY = "log.input.topic";
	public static final String LOG_INDEXING_BATCH_SIZE = "log.indexing.batch.size.seconds";
	public static final String PROPS_FILE_KEY = "properties.file";

	public static void main(String[] args) throws Exception {

		ParameterTool params = ParameterTool.fromArgs(args);

		if (params.has(PROPS_FILE_KEY)) {
			params = ParameterTool.fromPropertiesFile(params.get(PROPS_FILE_KEY)).mergeWith(params);
		}

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		KeyedStream<Map<String, String>, String> logStream = readLogStream(params, env);

		DataStream<UpdateResponse> logIndexResponse = logStream
				.timeWindow(Time.seconds(params.getLong(LOG_INDEXING_BATCH_SIZE, 10)))
				.apply(new SolrIndexer(params))
				.name("Solr Indexer")
				.uid("Solr Indexer");

		handleIndexingErrors(logIndexResponse);

		env.execute("Solr log indexer");
	}

	/**
	 * Handle the stream of {@link UpdateResponse}s coming from our indexing logic.
	 * In this prototype we will only log errors.
	 *
	 * @param logIndexResponse Stream of {@link UpdateResponse}, one for each window of logs.
	 */
	public static void handleIndexingErrors(DataStream<UpdateResponse> logIndexResponse) {
		logIndexResponse
				.addSink(new SinkFunction<UpdateResponse>() {
					@Override
					public void invoke(UpdateResponse resp, Context context) throws Exception {
						Exception e = resp.getException();
						if (e != null) {
							LOG.error("Error while indexing logs", e);
						}
					}
				})
				.name("Error Logger");
	}

	/**
	 * Read the JSON formatted log stream from Kafka and convert it to a {@link DataStream} of Maps
	 * for easier handling.
	 *
	 * @param params Application parameters
	 * @param env    {@link StreamExecutionEnvironment} of the current job.
	 * @return Parsed and transformed stream of logs.
	 */
	public static KeyedStream<Map<String, String>, String> readLogStream(ParameterTool params, StreamExecutionEnvironment env) {
		FlinkKafkaConsumer<String> logSource = new FlinkKafkaConsumer<>(
				params.getRequired(LOG_INPUT_TOPIC_KEY), new SimpleStringSchema(),
				Utils.readKafkaProperties(params));

		return env.addSource(logSource)
				.name("Kafka Log Source")
				.uid("Kafka Log Source")
				.flatMap(new LogParser())
				.name("JSON parser")
				.keyBy(map -> map.get(LogParser.YARN_CONTAINER_ID_KEY));
	}
}
