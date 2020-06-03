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

import java.io.File;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.io.IOCase;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.commons.io.filefilter.IOFileFilter;

import com.wday.prism.dataset.PrismDataLoaderMain;
import com.wday.prism.dataset.api.DataAPIConsumer;
import com.wday.prism.dataset.api.types.DatasetType;
import com.wday.prism.dataset.api.types.GetBucketResponseType;
import com.wday.prism.dataset.file.loader.DatasetLoaderException;
import com.wday.prism.dataset.file.loader.FolderUploader;
import com.wday.prism.dataset.file.schema.FileSchema;
import com.wday.prism.dataset.util.APIEndpoint;

public class LoadFolder {
	
	static final String tableName = "Table_virus_scan_test";
	static final String serviceEndpointURL = "https://wd2-impl-services1.workday.com/ccx/api/v2/informatica_pt2";
	static final String oauthClientId =	"ODFjZDBjZDMtMjEzYi00Zjc1LThiNTItYWEzYTkwNjNkMDIw";
	static final String oauthClientSecret =	"xdrlhgs1qg7d2ifwdfizlnaaeo8sg7o4zexusor3m3qhjyps5esf8ngi94qikznb9ln8dppjrsjdjwxhf7f2j2epx30p9swh8c7";
	static final String oauthRefreshToken =  "t07cjyccyc7tq32d7t435ratkt8i71tmyktnlb5iwicky7fihegi260py363y3ukrtppb829b03b20f490uwp7w11nvxnpar4km";
	static final File uploadFile = new File("/Users/puneet.gupta/Downloads/data/virus_scan_test");
	static final File schemaFile = new File("/Users/puneet.gupta/Downloads/data/virus_scan_test_schema.json");
	
	public static void main(String[] args) {

		try {
			
			APIEndpoint endpoint = APIEndpoint.getAPIEndpoint(serviceEndpointURL);
			String accessToken = PrismDataLoaderMain.getTenantCredentials(endpoint, oauthClientId, oauthClientSecret,oauthRefreshToken);
			@SuppressWarnings("unused")
			List<GetBucketResponseType> bucketList = DataAPIConsumer.listBuckets(endpoint.tenantURL, endpoint.apiVersion, endpoint.tenant, accessToken, System.out);
			List<DatasetType> datasetList = DataAPIConsumer.listDatasets(endpoint.tenantURL, endpoint.apiVersion, endpoint.tenant, accessToken,
					tableName, System.out);
			if (datasetList == null || datasetList.size() == 0) {
				System.out.println("Table "+tableName+" not found");//Table_virus_scan_test
				return;
			}
			
			String tableId = datasetList.get(0).getId();
			
			if (!uploadFile.exists()) {
				System.out.println("Error: File {" + uploadFile.getAbsolutePath() + "} not found");
				throw new DatasetLoaderException("File {" + uploadFile.getAbsolutePath() + "} not found");
			}
			
			List<File> inputFiles = new LinkedList<File>();

			if(uploadFile.isDirectory())
			{	
				try {
					IOFileFilter suffixFileFilter1 = FileFilterUtils.suffixFileFilter(".csv.gz", IOCase.INSENSITIVE);
					IOFileFilter suffixFileFilter2 = FileFilterUtils.suffixFileFilter(".csv", IOCase.INSENSITIVE);
					IOFileFilter orFileFilter = FileFilterUtils.or(suffixFileFilter1, suffixFileFilter2);
					File[] files = FolderUploader.getFiles(uploadFile, orFileFilter);
					if (files != null) {				
						for (File file : files) {

							if (file == null) {
								System.out.println("Error: Input File is null");
								throw new DatasetLoaderException("Input File is null");
							}
							
							if (file.getName().toLowerCase().endsWith("__err.csv"))
								continue;
							

							if (!file.exists()) {
								System.out.println("Error: File {" + file.getAbsolutePath() + "} not found");
								throw new DatasetLoaderException("File {" + file.getAbsolutePath() + "} not found");
							}

							if (file.length() == 0) {
								System.out.println("Error: File {" + file.getAbsolutePath() + "} is empty");
								throw new DatasetLoaderException("Error: File {" + file.getAbsolutePath() + "} is empty");
							}
							inputFiles.add(file);
						}
					}
				} catch (Throwable t) {
					t.printStackTrace();
					throw new DatasetLoaderException(t.toString());
				}
				
				if(inputFiles.size()==0)
				{
					System.out.println("Input File {"+uploadFile+"} does not contain any valid .csv or .csv.gz files");
					throw new DatasetLoaderException("Input File {"+uploadFile+"} does not contain any valid .csv or .csv.gz files");				
				}			
			}
			
			FileSchema schema = FileSchema.load(schemaFile, null, System.out);
			
			String hdrId = DataAPIConsumer.createBucket(endpoint.tenantURL, endpoint.apiVersion, endpoint.tenant, accessToken, tableName, tableId,
					schema, "REPLACE", System.out);
			
			if (hdrId == null || hdrId.isEmpty()) {
				throw new DatasetLoaderException("Error: failed to create Bucket for Dataset: " + tableName);
			}

			long startTime = System.currentTimeMillis();
			DataAPIConsumer.uploadDirToBucket(endpoint.tenantURL, endpoint.apiVersion, endpoint.tenant, accessToken, hdrId, uploadFile, System.out, null);
			long endTime = System.currentTimeMillis();
			long uploadTime = endTime - startTime;

			boolean status = DataAPIConsumer.completeBucketWithRetry(endpoint.tenantURL, endpoint.apiVersion, endpoint.tenant, accessToken, hdrId, System.out);
			String statusMessage = null;
			startTime = System.currentTimeMillis();
			while (status) {
				GetBucketResponseType serverStatus = DataAPIConsumer.getBucket(endpoint.tenantURL, endpoint.apiVersion, endpoint.tenant,
						accessToken, hdrId, System.out);
				if (serverStatus != null) {
					// Bucket state can be: New, Processing, Loading, Success, Failed
					if (serverStatus.getBucketState().equalsIgnoreCase("Success")) {
						statusMessage = serverStatus.getErrorMessage();
						break;
					} else if (serverStatus.getBucketState().equalsIgnoreCase("Warning")) {
						statusMessage = serverStatus.getErrorMessage();
						break;
					} else if (serverStatus.getBucketState().equalsIgnoreCase("Processing")) {
						if (System.currentTimeMillis() - startTime > 15*60*1000) {
							status = false;
							throw new DatasetLoaderException(
									"Bucket {" + hdrId + "} did not finish processing in time. Giving up...");
						}
						Thread.sleep(5000);
						continue;
					} else // status can be (New, Loading, Failed) all are invalid states at this point
					{
						status = false;
						if (serverStatus.getErrorMessage() != null)
							throw new DatasetLoaderException(serverStatus.getErrorMessage());
						break;
					}

				}
			}
			
			System.out.println("Uplaod Status: "+statusMessage);
			
			DataAPIConsumer.shutdown();
			
		} catch (Throwable t) {
			t.printStackTrace();
		}

	}

}
