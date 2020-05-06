/*
 * =========================================================================================
 * Copyright (c) 2018 Workday, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Contributors:
 * Each Contributor (“You”) represents that such You are legally entitled to submit any 
 * Contributions in accordance with these terms and by posting a Contribution, you represent
 * that each of Your Contribution is Your original creation.   
 *
 * You are not expected to provide support for Your Contributions, except to the extent You 
 * desire to provide support. You may provide support for free, for a fee, or not at all. 
 * Unless required by applicable law or agreed to in writing, You provide Your Contributions 
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or 
 * implied, including, without limitation, any warranties or conditions of TITLE, 
 * NON-INFRINGEMENT, MERCHANTABILITY, or FITNESS FOR A PARTICULAR PURPOSE.
 * =========================================================================================
 */
package com.wday.prism.dataset.util;

import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.params.HttpParams;

public class HttpUtils {

	public static HttpClient getHttpClient() throws UnknownHostException {
		
		/*
		HttpParams params = new BasicHttpParams();
		HttpConnectionParams.setConnectionTimeout(params, 60000);
		HttpConnectionParams.setSoTimeout(params, 60000);

		HttpClient httpClient = new DefaultHttpClient(params);
		*/
		
		HttpClientBuilder httpClientBuilder = HttpClientBuilder.create();
		CloseableHttpClient httpClient = httpClientBuilder.build();
		return httpClient;
	}

	/*
	 * public static RequestConfig getRequestConfig() { RequestConfig requestConfig
	 * = RequestConfig.custom() .setSocketTimeout(60000) .setConnectTimeout(60000)
	 * .setConnectionRequestTimeout(60000) .build(); return requestConfig; }
	 */

	public static Map<String, String> getQueryParameters(String url) throws URISyntaxException {
		url = url.replace("#", "?");
		Map<String, String> params = new HashMap<>();
		List<NameValuePair> kvp = new URIBuilder(url).getQueryParams();
		for (NameValuePair j : kvp) {
			params.put(j.getName(), j.getValue());
		}
		return params;
	}

}
