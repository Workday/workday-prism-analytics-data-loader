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

import java.io.File;
import java.text.NumberFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import com.sforce.soap.partner.PartnerConnection;
import com.wday.prism.dataset.constants.Constants;
import com.wday.prism.dataset.file.schema.FieldType;

public class SfdcExtracter {

	public static final NumberFormat nf = NumberFormat.getIntegerInstance();

	public static void extract(String rootSObject,String datasetAlias, PartnerConnection partnerConnection, int rowLimit) throws Exception
	{
		if(SfdcUtils.excludedObjects.contains(rootSObject))
		{
			System.out.println("Error: Object {"+rootSObject+"} not supported");
			return;
		}

		Map<String,String> selectedObjectList = new LinkedHashMap<String,String>();

		Map<String,String> objectList = SfdcUtils.getObjectList(partnerConnection, Pattern.compile("\\b"+rootSObject+"\\b"), false);
		System.out.println("\n");
		if(objectList==null || objectList.size()==0)
		{
			System.out.println("Error: Object {"+rootSObject+"} not found");
			return;
		}
		if( objectList.size()>1)
		{
			System.out.println("Error: More than one Object found {"+objectList.keySet()+"}");
			return;
		}
		selectedObjectList.putAll(objectList);
		
		LinkedHashMap<String,List<FieldType>> objectFieldMap = previewData(selectedObjectList, partnerConnection, new File(Constants.dataDirName), rowLimit);
		System.out.println("\n");
	}
	

	
	public static LinkedHashMap<String,List<FieldType>> previewData(Map<String,String> selectedObjectList, PartnerConnection partnerConnection, File dataDir, int rowLimit) throws Exception
	{
		LinkedHashMap<String,List<FieldType>> wfdef = new LinkedHashMap<String,List<FieldType>>();
		for(String alias:selectedObjectList.keySet())
		{	
			if(!wfdef.containsKey(selectedObjectList.get(alias)))
			{
				List<FieldType> fields = SfdcUtils.getFieldList(selectedObjectList.get(alias), partnerConnection, false);
				wfdef.put(selectedObjectList.get(alias), fields);
				try
				{
					SfdcUtils.read(partnerConnection, selectedObjectList.get(alias), fields, rowLimit,dataDir);
				}catch(Throwable t)
				{
					t.printStackTrace();
				}
			}
		}
		return wfdef;
	}

	
}
