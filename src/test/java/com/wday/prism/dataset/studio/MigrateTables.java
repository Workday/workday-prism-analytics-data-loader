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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import com.wday.prism.dataset.PrismDataLoaderMain;
import com.wday.prism.dataset.api.DataAPIConsumer;
import com.wday.prism.dataset.api.types.CreateDatasetRequestType;
import com.wday.prism.dataset.api.types.DatasetType;
import com.wday.prism.dataset.api.types.GetBucketResponseType;
import com.wday.prism.dataset.constants.Config;
import com.wday.prism.dataset.util.APIEndpoint;

public class MigrateTables {
	
	static final String[] tablesToMigrate = {"SFDC_Product2_BDS"};
	
	public static void main(String[] args) {

		try {
			
			List<String> candidatetables = Arrays.asList(tablesToMigrate);
			
			Config conf = Config.getSystemConfig();
			
			APIEndpoint endpoint = APIEndpoint.getAPIEndpoint(conf.serviceEndpointURL);
			String accessToken = PrismDataLoaderMain.getTenantCredentials(endpoint, conf.oauthClientId, conf.oauthClientSecret,
					conf.oauthRefreshToken);
			@SuppressWarnings("unused")
			List<GetBucketResponseType> bucketList = DataAPIConsumer.listBuckets(endpoint.tenantURL, endpoint.apiVersion, endpoint.tenant, accessToken, System.out);


			APIEndpoint endpoint2 = APIEndpoint.getAPIEndpoint(conf.serviceEndpointURL2);
			String accessToken2 = PrismDataLoaderMain.getTenantCredentials(endpoint2, conf.oauthClientId2, conf.oauthClientSecret2,
					conf.oauthRefreshToken2);
			@SuppressWarnings("unused")
			List<GetBucketResponseType> bucketList2 = DataAPIConsumer.listBuckets(endpoint2.tenantURL, endpoint2.apiVersion, endpoint2.tenant, accessToken2, System.out);
			
			List<DatasetType> datasetList = DataAPIConsumer.listDatasets(endpoint.tenantURL, endpoint.apiVersion, endpoint.tenant, accessToken,
					null, System.out);
			if (datasetList == null || datasetList.size() == 0) {
				System.out.println("No Tables found to migrate in tenant: "+endpoint.tenant);
			}

			List<DatasetType> datasetList2 = DataAPIConsumer.listDatasets(endpoint2.tenantURL, endpoint2.apiVersion, endpoint2.tenant, accessToken2,
					null, System.out);
			if (datasetList2 == null || datasetList2.size() == 0) {
				System.out.println("No Tables found in tenant: "+endpoint2.tenant);
			}
			
			HashMap<String,String>  datasetMap = new HashMap<String,String>(datasetList2.size());
			for(DatasetType dataset:datasetList2)
			{
				datasetMap.put(dataset.getName(),dataset.getId());
			}

			for(DatasetType dataset:datasetList)
			{
				if(!candidatetables.contains(dataset.getName()))
					continue;
				
				try
				{
					DatasetType datasetDetails = DataAPIConsumer.describeDataset(endpoint.tenantURL, endpoint.apiVersion, endpoint.tenant, accessToken,
							dataset.getId(), System.out);
					CreateDatasetRequestType createDatasetRequestType = new CreateDatasetRequestType(datasetDetails);
					
					if(endpoint.tenant.equalsIgnoreCase(endpoint2.tenant))
					{
						createDatasetRequestType.setName("Table_"+createDatasetRequestType.getName());
						createDatasetRequestType.setDisplayName(createDatasetRequestType.getName());
					}
					
					if(datasetMap.containsKey(createDatasetRequestType.getName()))
					{
						DataAPIConsumer.updateDataset(endpoint2.tenantURL, endpoint2.apiVersion, endpoint2.tenant, accessToken2, datasetMap.get(createDatasetRequestType.getName()), createDatasetRequestType, System.out);		
					}else
					{
						DataAPIConsumer.createDataset(endpoint2.tenantURL, endpoint2.apiVersion, endpoint2.tenant, accessToken2, createDatasetRequestType, System.out);
					}
				}catch(Throwable t)
				{
					t.printStackTrace();
				}
			}
			
		
		} catch (Throwable t) {
			t.printStackTrace();
		}

	}

}
