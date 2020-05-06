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
package com.wday.prism.dataset.constants;

import java.nio.charset.CodingErrorAction;

import org.apache.commons.io.FileUtils;

public class Constants {

	public static final int DEFAULT_BUFFER_SIZE = 8 * 1024 * 1024;

	public static final int MAX_CONCURRENT_LOADS = 2;

	public static final int MAX_CONCURRENT_FILE_LOADS = 3;

	public static final int MAX_NUM_FILE_PARTS_IN_A_BUCKET = 100;

	public static final long COMPRESSED_FILE_PART_LENGTH = 128 * FileUtils.ONE_MB; // 128MB

	public static final long MAX_COMPRESSED_FILE_PART_LENGTH = 256 * FileUtils.ONE_MB; // 256MB

	public static final long MAX_UNCOMPRESSED_FILE_LENGTH = 24 * FileUtils.ONE_GB; // 24GB

	public static final long MAX_COMPRESSED_FILE_LENGTH = 8 * FileUtils.ONE_GB; // 24GB
	
	public static final String API_VERSION_1 = "v1";
	
	public static final String API_VERSION_2 = "v2";

	public static CodingErrorAction codingErrorAction = CodingErrorAction.REPORT;

	public static boolean debug = false;

	public static final String errorCsvParam = "ERROR_CSV";
	public static final String metadataJsonParam = "METADATA_JSON";
	public static final String hdrIdParam = "HEADER_ID";
	public static final String serverStatusParam = "SERVER_STATUS";

	public static final int max_error_threshhold = 10000;
	
	public static final String logsDirName = "logs";
	public static final String successDirName = "success";
	public static final String errorDirName = "error";
	public static final String dataDirName = "data";
	public static final String configDirName = "config";
	public static final String configFileName = "settings.json";
	public static final String preferenceFileName = "preferences.json";


}
