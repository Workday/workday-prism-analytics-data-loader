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
package com.wday.prism.dataset.file.loader;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.concurrent.atomic.AtomicBoolean;

import com.wday.prism.dataset.api.DataAPIConsumer;
import com.wday.prism.dataset.monitor.Session;
import com.wday.prism.dataset.monitor.ThreadContext;

public class FileUploadWorker implements Runnable {

	private final Session session;

	private final PrintStream logger;
	private File uploadFile = null;
	String bucketId = null;
	private String tenantURL = null;
	private String apiVersion = null;
	private String tenantName = null;
	private String accessToken = null;
	private volatile AtomicBoolean uploadStatus = new AtomicBoolean(false);
	private volatile AtomicBoolean isDone = new AtomicBoolean(false);

	public FileUploadWorker(String tenantURL, String apiVersion, String tenantName, String accessToken, String bucketId, File uploadFile, PrintStream logger, Session session)
			throws IOException {
		this.session = session;
		this.tenantURL = tenantURL;
		this.tenantName = tenantName;
		this.apiVersion = apiVersion;
		this.accessToken = accessToken;
		this.logger = logger;
		this.uploadFile = uploadFile;
		this.bucketId = bucketId;
	}

	@Override
	public void run() {
		boolean status = false;
		try {
			
			if(session!=null)
			{
				ThreadContext threadContext = ThreadContext.get();
				threadContext.setSession(session);
			}
			
			try {
				System.out.println("START Uploading file {" + this.uploadFile + "} to Bucket: " + bucketId);
				status = DataAPIConsumer.uploadStreamToBucket(tenantURL, apiVersion, tenantName, accessToken, bucketId, new FileInputStream(uploadFile),
						uploadFile.getName(), logger);
			} catch (Throwable e) {
				e.printStackTrace(logger);
			} finally {
				System.out.println("END Uploading file {" + this.uploadFile + "} to Bucket: " + bucketId + " Status: "
						+ status);
			}
		} finally {
			uploadStatus.set(status);
			isDone.set(true);
		}
	}

	public boolean isDone() {
		return isDone.get();
	}

	public boolean isUploadStatus() {
		return uploadStatus.get();
	}

}
