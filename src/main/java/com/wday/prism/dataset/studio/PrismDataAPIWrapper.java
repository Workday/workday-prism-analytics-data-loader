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
package com.wday.prism.dataset.studio;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.charset.UnsupportedCharsetException;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.zip.GZIPInputStream;

import org.apache.http.client.ClientProtocolException;

import com.wday.prism.dataset.api.DataAPIConsumer;
import com.wday.prism.dataset.api.types.DatasetType;
import com.wday.prism.dataset.api.types.GetBucketResponseType;
import com.wday.prism.dataset.constants.Constants;
import com.wday.prism.dataset.file.loader.DatasetLoaderException;
import com.wday.prism.dataset.file.schema.FileSchema;
import com.wday.prism.dataset.util.APIEndpoint;

/**
 * @author puneet.gupta
 *
 */
public class PrismDataAPIWrapper {

	private static final long maxWaitTime = 5 * 60 * 1000L;

	public static String authenticate(String workdayRESTAPIEndpoint, String clientID, String clientSecret,
			String refreshToken)
			throws DatasetLoaderException, ClientProtocolException, IOException, URISyntaxException {
		APIEndpoint endpoint = APIEndpoint.getAPIEndpoint(workdayRESTAPIEndpoint);
		return DataAPIConsumer.getAccessToken(endpoint.tenantURL, endpoint.apiVersion, endpoint.tenant, clientID,
				clientSecret, refreshToken, System.out);
	}

	public static String detectSchema(InputStream csvFile, String fileName,char delim, String fileCharset)
			throws IOException, DatasetLoaderException {
		boolean isGzipCompressed = fileName != null && fileName.endsWith(".gz");
		FileSchema schema = FileSchema.createAutoSchema((isGzipCompressed?(new GZIPInputStream(csvFile,Constants.DEFAULT_BUFFER_SIZE)):csvFile), delim,
				fileCharset != null ? Charset.forName(fileCharset) : null, System.out);
		return schema.toString();
	}

	public static String startLoad(String workdayRESTAPIEndpoint, String accessToken, String dataset,
			String schemaString, boolean createDataset)
			throws DatasetLoaderException, ClientProtocolException, IOException, URISyntaxException {
		APIEndpoint endpoint = APIEndpoint.getAPIEndpoint(workdayRESTAPIEndpoint);
		FileSchema schema = FileSchema.load(schemaString, System.out);
		try {
			String datasetId = null;
			List<DatasetType> datasetList = DataAPIConsumer.listDatasets(endpoint.tenantURL, endpoint.apiVersion,
					endpoint.tenant, accessToken, null, System.out);
			if (datasetList != null && datasetList.size() != 0) {
				for (DatasetType list : datasetList) {
					if (dataset.equalsIgnoreCase(list.getName())) {
						datasetId = list.getId();
						break;
					}
				}
			}
			if (datasetId == null && createDataset == true) {
				datasetId = DataAPIConsumer.createDataset(endpoint.tenantURL, endpoint.apiVersion, endpoint.tenant,
						accessToken, dataset, dataset, schema, System.out);
			}
			String rowId = DataAPIConsumer.createBucket(endpoint.tenantURL, endpoint.apiVersion, endpoint.tenant,
					accessToken, dataset, datasetId, schema, "Replace", System.out);
			if (rowId == null)
				throw new DatasetLoaderException("Failed to create Bucket for dataset: " + dataset);
			return rowId;
		} catch (Throwable e) {
			e.printStackTrace(System.out);
			throw new DatasetLoaderException("Failed to create Bucket: " + e.getMessage());
		}

	}

	public static boolean loadFile(String workdayRESTAPIEndpoint, String accessToken, String bucketId, String fileName,
			InputStream fileinputStream) throws DatasetLoaderException, ClientProtocolException, IOException,
			URISyntaxException, NoSuchAlgorithmException {
		APIEndpoint endpoint = APIEndpoint.getAPIEndpoint(workdayRESTAPIEndpoint);
		return DataAPIConsumer.uploadStreamToBucket(endpoint.tenantURL, endpoint.apiVersion, endpoint.tenant,
				accessToken, bucketId, fileinputStream, fileName, System.out);
	}

	public static boolean completeLoad(String workdayRESTAPIEndpoint, String accessToken, String bucketId,
			boolean waitForComplete)
			throws DatasetLoaderException, ClientProtocolException, IOException, URISyntaxException, UnsupportedCharsetException, InterruptedException {
		APIEndpoint endpoint = APIEndpoint.getAPIEndpoint(workdayRESTAPIEndpoint);
		boolean status = DataAPIConsumer.completeBucketWithRetry(endpoint.tenantURL, endpoint.apiVersion, endpoint.tenant,
				accessToken, bucketId, System.out);
		if (!waitForComplete)
			return status;
		long startTime = System.currentTimeMillis();
		while (status) {

			GetBucketResponseType serverStatus = DataAPIConsumer.getBucket(endpoint.tenantURL, endpoint.apiVersion,
					endpoint.tenant, accessToken, bucketId, System.out);
			if (serverStatus != null) {
				if (serverStatus.getBucketState().equalsIgnoreCase("Success")) {
					System.out.println("Bucket {" + bucketId + "} completed succesfully");
					break;
				} else if (serverStatus.getBucketState().equalsIgnoreCase("Processing")) {
					if (System.currentTimeMillis() - startTime > maxWaitTime) {
						System.out.println(
								"Bucket {" + bucketId + "} has not finished processing. Cannot wait any longer...");
						status = false;
						break;
					}
					try {
						Thread.sleep(3000);
					} catch (InterruptedException e) {
						e.printStackTrace();
						status = false;
						break;
					}
					continue;
				} else // status can be (New, Loading, Failed) all are invalid states at this point
				{
					System.out.println("Invalid Bucket state {" + serverStatus.getBucketState() + "} ");

					status = false;
					if (serverStatus.getErrorMessage() != null)
						throw new DatasetLoaderException(serverStatus.getErrorMessage());
					break;
				}
			}
		}
		return status;
	}

	public static DatasetType describe(String workdayRESTAPIEndpoint, String accessToken, String dataset)
			throws DatasetLoaderException, ClientProtocolException, IOException, URISyntaxException {
		APIEndpoint endpoint = APIEndpoint.getAPIEndpoint(workdayRESTAPIEndpoint);
		String datasetId = null;
		List<DatasetType> datasetList = DataAPIConsumer.listDatasets(endpoint.tenantURL, endpoint.apiVersion,
				endpoint.tenant, accessToken, null, System.out);
		if (datasetList != null && datasetList.size() != 0) {
			for (DatasetType list : datasetList) {
				if (dataset.equalsIgnoreCase(list.getName())) {
					datasetId = list.getId();
					break;
				}
			}
		}
		if (datasetId != null) {
			return DataAPIConsumer.describeDataset(endpoint.tenantURL, endpoint.apiVersion, endpoint.tenant,
					accessToken, datasetId, System.out);
		}
		throw new DatasetLoaderException("Dataset {" + dataset + "} not found");
	}
}
