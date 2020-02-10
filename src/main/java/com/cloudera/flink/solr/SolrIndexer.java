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

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.Krb5HttpClientBuilder;
import org.apache.solr.client.solrj.impl.SolrHttpClientBuilder;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Window function for indexing batches of log messages (already parsed by {@link LogParser}) into Solr.
 * We don't apply any transformation logic, the string maps are converted to {@link org.apache.solr.common.SolrDocument}s
 * and indexed as they are.
 */
public class SolrIndexer extends RichWindowFunction<Map<String, String>, UpdateResponse, String, TimeWindow> {

	public static final String SOLR_URLS_KEY = "solr.urls";
	public static final String SOLR_COLLECTION_KEY = "solr.collection";

	public static final String SOLR_SSL_LOCATION_KEY = "solr.ssl.truststore.location";
	public static final String SOLR_SSL_PWD_KEY = "solr.ssl.truststore.password";

	private final List<String> solrUrls;
	private final String solrCollection;
	private final String trustStorePath;
	private final String trustStorePassword;

	private transient SolrClient solrClient;

	public SolrIndexer(ParameterTool params) {
		solrUrls = Lists.newArrayList(params.getRequired(SOLR_URLS_KEY).split(","));
		solrCollection = params.getRequired(SOLR_COLLECTION_KEY);
		trustStorePath = params.get(SOLR_SSL_LOCATION_KEY);
		trustStorePassword = params.get(SOLR_SSL_PWD_KEY);
	}

	@Override
	public void apply(String k, TimeWindow w, Iterable<Map<String, String>> logs, Collector<UpdateResponse> output) throws Exception {
		output.collect(solrClient.add(solrCollection, mapToSolrDocuments(logs)));
	}

	private List<SolrInputDocument> mapToSolrDocuments(Iterable<Map<String, String>> logs) throws IOException {
		List<SolrInputDocument> docs = new ArrayList<>();
		for (Map<String, String> json : logs) {
			SolrInputDocument doc = new SolrInputDocument();
			json.forEach(doc::addField);
			docs.add(doc);
		}
		return docs;
	}

	@Override
	public void open(Configuration config) {
		if (trustStorePath != null) {
			System.setProperty("javax.net.ssl.trustStore", trustStorePath);
		}

		if (trustStorePassword != null) {
			System.setProperty("javax.net.ssl.trustStorePassword", trustStorePassword);
		}

		Krb5HttpClientBuilder krbBuild = new Krb5HttpClientBuilder();
		SolrHttpClientBuilder kb = krbBuild.getBuilder();
		HttpClientUtil.setHttpClientBuilder(kb);
		solrClient = new CloudSolrClient.Builder(solrUrls).build();
	}

	@Override
	public void close() throws IOException {
		if (solrClient != null) {
			solrClient.close();
			solrClient = null;
		}
	}
}
