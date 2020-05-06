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
package com.wday.prism.dataset.api;

import java.io.File;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.charset.UnsupportedCharsetException;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.IOCase;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.apache.commons.io.input.CountingInputStream;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.entity.mime.MultipartEntity;
import org.apache.http.entity.mime.content.InputStreamBody;
import org.apache.http.message.BasicNameValuePair;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.wday.prism.dataset.api.types.CreateBucketRequestType;
import com.wday.prism.dataset.api.types.CreateDatasetRequestType;
import com.wday.prism.dataset.api.types.DatasetType;
import com.wday.prism.dataset.api.types.GetBucketResponseType;
import com.wday.prism.dataset.api.types.GetDatasetsResponseType;
import com.wday.prism.dataset.api.types.ListBucketsResponseType;
import com.wday.prism.dataset.constants.Constants;
import com.wday.prism.dataset.file.loader.DatasetLoaderException;
import com.wday.prism.dataset.file.loader.FileUploadWorker;
import com.wday.prism.dataset.file.schema.FileSchema;
import com.wday.prism.dataset.monitor.Session;
import com.wday.prism.dataset.util.FasterBufferedInputStream;
import com.wday.prism.dataset.util.FileUtilsExt;
import com.wday.prism.dataset.util.GZipInputStreamDeflater;
import com.wday.prism.dataset.util.HttpUtils;

/**
 * @author puneet.gupta
 *
 */
public class DataAPIConsumer {

	/** The Constant nf. */
	public static final NumberFormat nf = NumberFormat.getIntegerInstance();
	public static final NumberFormat nf2 = NumberFormat.getInstance();
	private static final int bufferSize = 65536;
	
	private static final int MAX_THREAD_POOL = 200;
	private static final ThreadPoolExecutor fileUploadExecutorPool = new ThreadPoolExecutor(Constants.MAX_CONCURRENT_FILE_LOADS,
			Constants.MAX_CONCURRENT_FILE_LOADS, 10, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(MAX_THREAD_POOL),
			Executors.defaultThreadFactory());


	public static boolean uploadDirToBucket(String tenantURL, String apiVersion, String tenantName, String accessToken,
			String bucketId, File bucketDir, PrintStream logger,Session session) throws URISyntaxException, ClientProtocolException,
			IOException, DatasetLoaderException, NoSuchAlgorithmException {

		if (bucketDir == null) {
			throw new IllegalArgumentException("inputFile cannot be blank");
		}

		if (bucketDir == null || !bucketDir.exists() || !bucketDir.canRead() || !bucketDir.isDirectory()) {
			throw new IllegalArgumentException("cannot open bucket dir {" + bucketDir + "}");
		}

		if (bucketId == null || bucketId.trim().equalsIgnoreCase("null") || bucketId.trim().isEmpty()) {
			throw new IllegalArgumentException("bucketId cannot be blank");
		}

		logger.println();

		IOFileFilter suffixFileFilter1 = FileFilterUtils.suffixFileFilter(".gz", IOCase.INSENSITIVE);
		File[] files = FileUtilsExt.getFiles(bucketDir, suffixFileFilter1, false);

		if (files == null || files.length == 0) {
			throw new DatasetLoaderException("No .gz files found in folder: " + bucketDir.getAbsolutePath());
		}

		if (files.length > Constants.MAX_NUM_FILE_PARTS_IN_A_BUCKET) {
			throw new DatasetLoaderException("Number of files in folder  {" + bucketDir.getAbsolutePath()
					+ "} is greater than max allowed {" + Constants.MAX_NUM_FILE_PARTS_IN_A_BUCKET + "}");
		}
		
		for (File file : files) {
			if (file.length() > Constants.MAX_COMPRESSED_FILE_PART_LENGTH) {
				throw new DatasetLoaderException("files {" + file.getAbsolutePath()
						+ "} size is greater than max allowed {" + Constants.MAX_COMPRESSED_FILE_PART_LENGTH + "}");
			}
		}

		LinkedHashMap<FileUploadWorker,String> fileUploadWorkers = new LinkedHashMap<FileUploadWorker,String>();


		for (File file : files) {
			try {
				if (session != null && session.isDone()) {
					throw new DatasetLoaderException("operation terminated on user request");
				}

				while (fileUploadExecutorPool.getQueue().size() >= MAX_THREAD_POOL) {
					System.out.println("There are too many file upload jobs in the queue, will wait before trying again");
					Thread.sleep(3000);
					if (session != null && session.isDone()) {
						throw new DatasetLoaderException("operation terminated on user request");
					}
				}
				FileUploadWorker worker = new FileUploadWorker(tenantURL, apiVersion, tenantName, accessToken, bucketId, file, logger, session);

				try {
					fileUploadExecutorPool.execute(worker);
					fileUploadWorkers.put(worker, file.getName());
				} catch (Throwable t) {
					System.out.println();
					t.printStackTrace(System.out);
				}

			} catch (Throwable e) {
				System.out.println();
				e.printStackTrace(System.out);
			} finally {
			}
		}

		try {
			return waitforAllWorkersToBeDone(fileUploadWorkers);
		} catch (InterruptedException e) {
			e.printStackTrace();
			return false;
		}
	}
	
	private static boolean waitforAllWorkersToBeDone(LinkedHashMap<FileUploadWorker,String> fileUploadWorkers)
			throws InterruptedException {
		boolean status = true;
		boolean working = true;
		while (working) {
			working = false;
			int sleepCount = 0;
			for (FileUploadWorker work : fileUploadWorkers.keySet()) {
				if (!work.isDone()) {
					working = true;
					if (sleepCount % 10 == 0)
						System.out.println("Waiting for FileUploadWorker {" + fileUploadWorkers.get(work) + "} to finish");
					Thread.sleep(3000);
					sleepCount++;
				} else {
					if(!work.isUploadStatus())
					{
						status = work.isUploadStatus();
					}
					System.out.println("FileUploadWorker {" + fileUploadWorkers.get(work) + "} Finished. status: "+(work.isUploadStatus()));
				}
			}
		}
		return status;
	}


	public static boolean uploadStreamToBucket(String tenantURL, String apiVersion, String tenantName,
			String accessToken, String bucketId, InputStream fileinputStream, String fileName, PrintStream logger)
			throws URISyntaxException, ClientProtocolException, IOException, DatasetLoaderException,
			NoSuchAlgorithmException {

		if (bucketId == null || bucketId.trim().equalsIgnoreCase("null") || bucketId.trim().isEmpty()) {
			throw new IllegalArgumentException("bucketId cannot be blank");
		}

		if (fileName == null || fileName.trim().isEmpty() ) {
			throw new IllegalArgumentException("file name cannot be blank");
		}
		
		
		FilterInputStream bis = new FasterBufferedInputStream(fileinputStream, bufferSize);
		if (!fileName.trim().toLowerCase().endsWith(".gz")) {
			bis = new GZipInputStreamDeflater(bis);
			fileName = fileName + ".gz";
		}

		CountingInputStream cis = new CountingInputStream(bis);
		MessageDigest md = MessageDigest.getInstance("MD5");
		DigestInputStream dis = new DigestInputStream(cis, md);

		logger.println();
		logger.println("START Uploading file: " + fileName + " to bucket: " + bucketId);
		long startTime = System.currentTimeMillis();

		HttpClient httpClient = HttpUtils.getHttpClient();
		// HttpUtils.getRequestConfig();

		HttpPost uploadFile = new HttpPost(
				getUploadEndpoint(tenantURL, apiVersion, tenantName, accessToken, bucketId, logger));

		InputStreamBody isb = new InputStreamBody(dis, fileName);
		MultipartEntity multipart = new MultipartEntity();
		multipart.addPart("File", isb);

		uploadFile.setEntity(multipart);
		uploadFile.addHeader("Authorization", "Bearer " + accessToken);

		HttpResponse response = httpClient.execute(uploadFile);
		long endTime = System.currentTimeMillis();

		HttpEntity responseEntity = response.getEntity();
		if (responseEntity == null) {
			logger.println("HTTP Response: " + response.getStatusLine().toString());
		} else {
			InputStream is = responseEntity.getContent();
			String responseString = IOUtils.toString(is, "UTF-8");
			logger.println("Response: " + responseString);
			is.close();

			int statusCode = response.getStatusLine().getStatusCode();
			if (statusCode != HttpStatus.SC_OK) {
				if (responseString != null && !responseString.isEmpty()) {
					System.err.println("FAILED Uploading file: " + fileName + ". Error: " + responseString);
					throw new DatasetLoaderException(
							"FAILED Uploading file: " + fileName + ". Error: " + responseString);
				} else {
					System.err.println("FAILED Uploading file: " + fileName + ". Error: " + response.getStatusLine().toString());
					throw new DatasetLoaderException(
							"FAILED Uploading file: " + fileName + ". Error: " + response.getStatusLine().toString());
				}
			} else {
				if (responseString != null && !responseString.isEmpty()) {
					ObjectMapper mapper = new ObjectMapper();
					mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
					Map<?, ?> res = mapper.readValue(responseString, Map.class);
					Object tmp = res.get("fileLength");
					int fileLength = 0;
					if (tmp != null && tmp instanceof Number) {
						fileLength = ((Number) tmp).intValue();
					}
					if (fileLength != cis.getCount()) {
						System.err.println("FAILED Uploading file: Bytes sent {" + cis.getByteCount()
						+ "} does not match bytes received {" + fileLength + "}");
						throw new DatasetLoaderException("FAILED Uploading file: Bytes sent {" + cis.getByteCount()
								+ "} does not match bytes received {" + fileLength + "}");
					} else {
						long uploadTime = endTime - startTime;
						BigDecimal bd = new BigDecimal(fileLength).divide(new BigDecimal(uploadTime), 2,
								RoundingMode.HALF_UP);
						logger.println(
								"END Uploading file: " + fileName + " uploadTime {" + nf.format(endTime - startTime)
										+ "} msecs, upload speed {" + bd.toPlainString() + "} kbps");
					}
				}

			}
		}
		// httpClient.close();
		return true;
	}

	public static List<DatasetType> listDatasets(String tenantURL, String apiVersion, String tenantName,
			String accessToken, String search, PrintStream logger)
			throws ClientProtocolException, URISyntaxException, IOException, DatasetLoaderException {
		logger.println();

		List<DatasetType> datasetList = new LinkedList<DatasetType>();

		HttpClient httpClient = HttpUtils.getHttpClient();
		// HttpUtils.getRequestConfig();

		URI u = new URI(tenantURL);
		int offset = 0;
		int pageNo = 0;
		long totalPages = 1;
		long datasetCount = 0;
		while (pageNo < totalPages) {

			URI listEMURI;
			if (search == null || search.trim().isEmpty() || search.trim().equalsIgnoreCase("null"))
				listEMURI = new URI(u.getScheme(), u.getUserInfo(), u.getHost(), u.getPort(),
						"/ccx/api/prismAnalytics/" + apiVersion + "/" + tenantName + "/datasets",
						"limit=100&offset=" + offset, null);
			else
				listEMURI = new URI(u.getScheme(), u.getUserInfo(), u.getHost(), u.getPort(),
						"/ccx/api/prismAnalytics/" + apiVersion + "/" + tenantName + "/datasets",
						"name=" + search, null);

			logger.println("Listing Datasets: " + listEMURI);

			HttpGet listEMPost = new HttpGet(listEMURI);

			// listEMPost.setConfig(requestConfig);
			listEMPost.addHeader("Authorization", "Bearer " + accessToken);
			HttpResponse emresponse = httpClient.execute(listEMPost);
			int statusCode = emresponse.getStatusLine().getStatusCode();
			if (statusCode != HttpStatus.SC_OK) {
				logger.println("Response: " + emresponse.getStatusLine().toString());
				if(statusCode == HttpStatus.SC_FORBIDDEN) //Fix for Server sending 403 when there are no datasets
					return datasetList;
				throw new DatasetLoaderException("Failed to List datasets " + emresponse.getStatusLine().toString());
			}

			HttpEntity emresponseEntity = emresponse.getEntity();
			InputStream emis = emresponseEntity.getContent();
			String emList = IOUtils.toString(emis, "UTF-8");
			// logger.println("List Dataset Response: " + emList);
			emis.close();
			// httpClient.close();

			if (emList != null && !emList.isEmpty()) {
				ObjectMapper mapper = new ObjectMapper();
				mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
				GetDatasetsResponseType res = mapper.readValue(emList, GetDatasetsResponseType.class);
				if (res != null && !res.data.isEmpty()) {
					datasetList.addAll(res.data);
					if (pageNo == 0) {
						datasetCount = res.total;
						totalPages = datasetCount / 100;
						if (datasetCount % 100 > 0)
							totalPages = totalPages + 1;
					}
					offset = offset + 100;
					pageNo++;
				} else {
					break;
				}
			} else {
				throw new DatasetLoaderException("Failed to List datasets " + emresponse.getStatusLine().toString());
			}
		} // end while
			// Collections.sort(datasetList, Collections.reverseOrder());
		if (datasetList.size() != datasetCount) {
			System.err.println("Query retuned {" + datasetCount + "} datasets but we only have {" + datasetList.size()
					+ "} datasets");
		}
		return datasetList;
	}

	public static DatasetType describeDataset(String tenantURL, String apiVersion, String tenantName,
			String accessToken, String datasetId, PrintStream logger)
			throws ClientProtocolException, URISyntaxException, IOException, DatasetLoaderException {
		// List<DatasetType> datasetList = new LinkedList<DatasetType>();
		logger.println();

		HttpClient httpClient = HttpUtils.getHttpClient();
		// HttpUtils.getRequestConfig();

		URI u = new URI(tenantURL);
		URI listEMURI = new URI(u.getScheme(), u.getUserInfo(), u.getHost(), u.getPort(),
				"/ccx/api/prismAnalytics/" + apiVersion + "/" + tenantName + "/datasets/" + datasetId + "/describe",
				null, null);

		logger.println("Describing Datasets: " + listEMURI);

		HttpGet listEMPost = new HttpGet(listEMURI);

		// listEMPost.setConfig(requestConfig);
		listEMPost.addHeader("Authorization", "Bearer " + accessToken);
		HttpResponse emresponse = httpClient.execute(listEMPost);
		int statusCode = emresponse.getStatusLine().getStatusCode();
		if (statusCode != HttpStatus.SC_OK) {
			logger.println("Response: " + emresponse.getStatusLine().toString());
			throw new DatasetLoaderException(
					"Failed to decribe dataset {" + datasetId + "}" + emresponse.getStatusLine().toString());
		}

		HttpEntity emresponseEntity = emresponse.getEntity();
		if (emresponseEntity == null) {
			throw new DatasetLoaderException(
					"Failed to decribe dataset {" + datasetId + "}" + emresponse.getStatusLine().toString());
		}

		InputStream emis = emresponseEntity.getContent();
		String emList = IOUtils.toString(emis, "UTF-8");
		logger.println("Describe Dataset Response: " + emList);
		emis.close();
		// httpClient.close();

		if (emList != null && !emList.isEmpty()) {
			ObjectMapper mapper = new ObjectMapper();
			mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
			GetDatasetsResponseType res = mapper.readValue(emList, GetDatasetsResponseType.class);
			if (res.data.size() == 1) {
				return res.data.get(0);
			}
		}
		throw new DatasetLoaderException(
				"Failed to decribe dataset {" + datasetId + "}" + emresponse.getStatusLine().toString());
	}

	public static String getAccessToken(String tenantURL, String apiVersion, String tenantName, String clientId,
			String clientSecret, String refreshToken, PrintStream logger)
			throws ClientProtocolException, IOException, URISyntaxException, DatasetLoaderException {
		logger.println();

		String access_token = null;
		HttpClient httpClient = HttpUtils.getHttpClient();
		// HttpUtils.getRequestConfig();

		URI u = new URI(tenantURL);
		String tokenUriString = "/ccx/oauth2/" + tenantName + "/token";
		URI tokenURI = new URI(u.getScheme(), u.getUserInfo(), u.getHost(), u.getPort(), tokenUriString, null, null);
		logger.println("Authenticating user to: " + tokenURI);
		HttpPost tok = new HttpPost(tokenURI);
		List<NameValuePair> params = new ArrayList<NameValuePair>();
		params.add(new BasicNameValuePair("grant_type", "refresh_token"));
		params.add(new BasicNameValuePair("refresh_token", refreshToken));
		tok.setEntity(new UrlEncodedFormEntity(params));
		// tok.setConfig(requestConfig);

		tok.addHeader("Authorization",
				"Basic " + new String(
						Base64.encodeBase64((clientId + ":" + clientSecret).getBytes(StandardCharsets.UTF_8)),
						StandardCharsets.UTF_8));
		tok.addHeader("content-type", "application/x-www-form-urlencoded");

		HttpResponse response = httpClient.execute(tok);
		int statusCode = response.getStatusLine().getStatusCode();

		if (statusCode != HttpStatus.SC_OK) {
			logger.println("Response: " + response.getStatusLine().toString());
			throw new DatasetLoaderException(
					"Could not authenticate with Workday: " + response.getStatusLine().toString());
		}
		
		String responseString = null;
		HttpEntity responseEntity = response.getEntity();
		if(responseEntity!=null)
		{
			InputStream is = responseEntity.getContent();
			responseString = IOUtils.toString(is, "UTF-8");
			is.close();
		}

		if (responseString != null && !responseString.isEmpty()) {
			ObjectMapper mapper = new ObjectMapper();
			mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
			Map res = mapper.readValue(responseString, Map.class);
			access_token = (String) res.get("access_token");
			if (access_token != null)
				return access_token;				
		}
		throw new DatasetLoaderException("Could not authenticate with Workday: " + response.getStatusLine().toString() + " :: "+responseString);
	}

	public static String createBucket(String tenantURL, String apiVersion, String tenantName, String accessToken,
			String datasetName, String datasetId, FileSchema schema, String operation, PrintStream logger)
			throws ClientProtocolException, IOException, URISyntaxException, DatasetLoaderException {
		logger.println();

		if (schema != null)
		{
			schema = FileSchema.load(schema.toString(), logger);
			schema.clearNumericParseFormat();
			schema.getParseOptions().setFieldsDelimitedBy(",");
			schema.getParseOptions().setHeaderLinesToIgnore(1);
		}
		
		if(apiVersion.equalsIgnoreCase(Constants.API_VERSION_1))
		{
			schema.getParseOptions().setFieldsEnclosingCharacterEscapedBy(null);
			schema.getParseOptions().setIgnoreLeadingWhitespaces(null);
			schema.getParseOptions().setIgnoreTrailingWhitespaces(null);
		}

		CreateBucketRequestType createBucketRequestType = new CreateBucketRequestType();
		createBucketRequestType.setDatasetId(datasetId);
		createBucketRequestType.setName(datasetName + "_" + UUID.randomUUID().toString().replaceAll("-", ""));
		createBucketRequestType.setUploadOperation(operation);
		createBucketRequestType.setSchema(schema);
		ObjectMapper mapper = new ObjectMapper();
		mapper.setSerializationInclusion(Include.NON_NULL);
		String bucketRequest = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(createBucketRequestType);

		HttpClient httpClient = HttpUtils.getHttpClient();

		URI u = new URI(tenantURL);
		String tokenUriString = "/ccx/api/prismAnalytics/" + apiVersion + "/" + tenantName + "/wBuckets";
		URI tokenURI = new URI(u.getScheme(), u.getUserInfo(), u.getHost(), u.getPort(), tokenUriString, null, null);
		logger.println("Creating new Bucket: " + tokenURI);
		HttpPost tok = new HttpPost(tokenURI);
		tok.setEntity(new StringEntity(bucketRequest, ContentType.APPLICATION_JSON));

		tok.addHeader("Authorization", "Bearer " + accessToken);
		tok.addHeader("content-type", ContentType.APPLICATION_JSON.toString());

		HttpResponse response = httpClient.execute(tok);
		int statusCode = response.getStatusLine().getStatusCode();
		HttpEntity responseEntity = response.getEntity();
		InputStream is = responseEntity.getContent();
		String responseString = IOUtils.toString(is, "UTF-8");
		logger.println("Create Bucket Response:" + responseString);
		is.close();

		if (statusCode != HttpStatus.SC_CREATED) {
			logger.println("Http Response: " + response.getStatusLine().toString());
			throw new DatasetLoaderException("Could not create Bucket: " + response.getStatusLine().toString());
		}

		if (responseString != null && !responseString.isEmpty()) {
			mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
			GetBucketResponseType res = mapper.readValue(responseString, GetBucketResponseType.class);
			if (res != null) {
				if (res.getId() != null) {
					return res.getId();
				}
			}
			throw new DatasetLoaderException("Could not create Bucket: " + responseString);
		} else {
			throw new DatasetLoaderException("Could not create Bucket: " + response.getStatusLine().toString());
		}
	}

	public static String createDataset(String tenantURL, String apiVersion, String tenantName, String accessToken,
			String datasetAlias,String displayName, FileSchema schema, PrintStream logger) throws URISyntaxException, UnsupportedCharsetException,
			ClientProtocolException, IOException, DatasetLoaderException {

		CreateDatasetRequestType createDatasetRequestType = null;
		if(!apiVersion.equalsIgnoreCase(Constants.API_VERSION_1))
		{
			createDatasetRequestType = new CreateDatasetRequestType(datasetAlias,schema.getFields());
			createDatasetRequestType.setDisplayName(displayName);
			schema.clearNumericParseFormat();
			createDatasetRequestType.setFields(schema.getFields());
		}else
		{
			createDatasetRequestType = new CreateDatasetRequestType(datasetAlias,null);
		}
		return createDataset(tenantURL, apiVersion, tenantName, accessToken, createDatasetRequestType, logger);
	}

	public static String createDataset(String tenantURL, String apiVersion, String tenantName, String accessToken,
			CreateDatasetRequestType createDatasetRequestType, PrintStream logger) throws URISyntaxException, UnsupportedCharsetException,
			ClientProtocolException, IOException, DatasetLoaderException {

		logger.println();

		ObjectMapper mapper = new ObjectMapper();
		mapper.setSerializationInclusion(Include.NON_NULL);

		HttpClient httpClient = HttpUtils.getHttpClient();
		// HttpUtils.getRequestConfig();

		URI u = new URI(tenantURL);
		String tokenUriString = "/ccx/api/prismAnalytics/" + apiVersion + "/" + tenantName + "/datasets";
		URI tokenURI = new URI(u.getScheme(), u.getUserInfo(), u.getHost(), u.getPort(), tokenUriString, null, null);
		logger.println(tokenURI);
		HttpPost tok = new HttpPost(tokenURI);
		tok.setEntity(
				new StringEntity(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(createDatasetRequestType),
						ContentType.APPLICATION_JSON));
		// tok.setConfig(requestConfig);

		tok.addHeader("Authorization", "Bearer " + accessToken);
		tok.addHeader("content-type", ContentType.APPLICATION_JSON.toString());

		HttpResponse response = httpClient.execute(tok);
		int statusCode = response.getStatusLine().getStatusCode();
		HttpEntity responseEntity = response.getEntity();
		InputStream is = responseEntity.getContent();
		String responseString = IOUtils.toString(is, "UTF-8");
		logger.println("Create Dataset Response:" + responseString);
		is.close();
		// httpClient.close();

		if (statusCode != HttpStatus.SC_CREATED) {
			logger.println("Http Response: " + response.getStatusLine().toString());
			throw new DatasetLoaderException("Could not create Dataset: " + response.getStatusLine().toString());
		}

		if (responseString != null && !responseString.isEmpty()) {
			mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
			DatasetType res = mapper.readValue(responseString, DatasetType.class);
			// mapper.writerWithDefaultPrettyPrinter().writeValue(logger, res);
			if (res != null) {
				if (res.getId() != null) {
					return res.getId();
				}
			}
			throw new DatasetLoaderException("Could not create Dataset: " + responseString);
		} else {
			throw new DatasetLoaderException("Could not create Dataset: " + response.getStatusLine().toString());
		}
	}
	
	public static String updateDataset(String tenantURL, String apiVersion, String tenantName, String accessToken, String datasetId,
			CreateDatasetRequestType createDatasetRequestType, PrintStream logger) throws URISyntaxException, UnsupportedCharsetException,
			ClientProtocolException, IOException, DatasetLoaderException {

		logger.println();
	
		
		ObjectMapper mapper = new ObjectMapper();
		mapper.setSerializationInclusion(Include.NON_NULL);

		HttpClient httpClient = HttpUtils.getHttpClient();
		// HttpUtils.getRequestConfig();

		URI u = new URI(tenantURL);
		String tokenUriString = "/ccx/api/prismAnalytics/" + apiVersion + "/" + tenantName + "/datasets/" + datasetId;
		URI tokenURI = new URI(u.getScheme(), u.getUserInfo(), u.getHost(), u.getPort(), tokenUriString, null, null);
		logger.println(tokenURI);
		HttpPut tok = new HttpPut(tokenURI);
		tok.setEntity(
				new StringEntity(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(createDatasetRequestType),
						ContentType.APPLICATION_JSON));
		// tok.setConfig(requestConfig);

		tok.addHeader("Authorization", "Bearer " + accessToken);
		tok.addHeader("content-type", ContentType.APPLICATION_JSON.toString());

		HttpResponse response = httpClient.execute(tok);
		int statusCode = response.getStatusLine().getStatusCode();
		HttpEntity responseEntity = response.getEntity();
		InputStream is = responseEntity.getContent();
		String responseString = IOUtils.toString(is, "UTF-8");
		logger.println("Update Dataset Response:" + responseString);
		is.close();
		// httpClient.close();

		if (statusCode != HttpStatus.SC_OK) {
			logger.println("Http Response: " + response.getStatusLine().toString());
			throw new DatasetLoaderException("Could not update Dataset: " + response.getStatusLine().toString());
		}

		if (responseString != null && !responseString.isEmpty()) {
			mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
			DatasetType res = mapper.readValue(responseString, DatasetType.class);
			// mapper.writerWithDefaultPrettyPrinter().writeValue(logger, res);
			if (res != null) {
				if (res.getId() != null) {
					return res.getId();
				}
			}
			throw new DatasetLoaderException("Could not update Dataset: " + responseString);
		} else {
			throw new DatasetLoaderException("Could not update Dataset: " + response.getStatusLine().toString());
		}
	}

	public static boolean completeBucketWithRetry(String tenantURL, String apiVersion, String tenantName,
			String accessToken, String bucketId, PrintStream logger)
			throws URISyntaxException, UnsupportedCharsetException, ClientProtocolException, IOException,
			DatasetLoaderException, InterruptedException {
		long maxWaitTime = 30 * 60 * 1000L;
		long startTime = System.currentTimeMillis();
		while (true) {
			if (completeBucket(tenantURL, apiVersion, tenantName, accessToken, bucketId, logger)) {
				return true;
			} else {
				if (System.currentTimeMillis() - startTime < maxWaitTime) {
					Thread.sleep(5000);
					continue;
				} else {
					break;
				}
			}
		}
		throw new DatasetLoaderException(
				"Could not complete Bucket {" + bucketId + "}. Giving up after trying for {" + maxWaitTime + "} msecs");
	}

	private static boolean completeBucket(String tenantURL, String apiVersion, String tenantName, String accessToken,
			String bucketId, PrintStream logger) throws URISyntaxException, UnsupportedCharsetException,
			ClientProtocolException, IOException, DatasetLoaderException {

		logger.println();

		HttpClient httpClient = HttpUtils.getHttpClient();
		// HttpUtils.getRequestConfig();
		ObjectMapper mapper = new ObjectMapper();
		mapper.setSerializationInclusion(Include.NON_NULL);

		URI u = new URI(tenantURL);
		String tokenUriString = "/ccx/api/prismAnalytics/" + apiVersion + "/" + tenantName + "/wBuckets/" + bucketId
				+ "/complete";
		URI tokenURI = new URI(u.getScheme(), u.getUserInfo(), u.getHost(), u.getPort(), tokenUriString, null, null);
		logger.println("Completing bucket: " + tokenURI);
		HttpPost tok = new HttpPost(tokenURI);
		tok.setEntity(new StringEntity(
				mapper.writerWithDefaultPrettyPrinter().writeValueAsString(new LinkedHashMap<String, String>()),
				ContentType.APPLICATION_JSON));
		// tok.setConfig(requestConfig);

		tok.addHeader("Authorization", "Bearer " + accessToken);
		tok.addHeader("content-type", ContentType.APPLICATION_JSON.toString());

		HttpResponse response = httpClient.execute(tok);
		int statusCode = response.getStatusLine().getStatusCode();
		HttpEntity responseEntity = response.getEntity();
		if (responseEntity == null) {
			throw new DatasetLoaderException("Could not complete Bucket {" + bucketId + "}. Server Response: "
					+ response.getStatusLine().toString());
		}
		InputStream is = responseEntity.getContent();
		String responseString = IOUtils.toString(is, "UTF-8");
		is.close();
		// httpClient.close();

		if (statusCode == HttpStatus.SC_CREATED) {
			if (responseString != null && !responseString.isEmpty()) {
				logger.println("Complete Bucket Response: " + responseString);
				mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
				Map res = mapper.readValue(responseString, Map.class);
				String id = (String) res.get("id");
				if (id != null && !id.isEmpty())
					return true;
				throw new DatasetLoaderException("Could not complete Bucket: " + responseString);
			}
		} else if (statusCode == HttpStatus.SC_BAD_REQUEST) {
			if (responseString != null && !responseString.isEmpty()) {
				logger.println("Complete Bucket Response: " + responseString);
				if(responseString.contains("different wBucket is in the Processing stage"))
				{
					return false;
				}
			}
		}
		logger.println("Response: " + response.getStatusLine().toString());
		throw new DatasetLoaderException("Could not complete Bucket: " + response.getStatusLine().toString());
	}

	public static List<GetBucketResponseType> listBuckets(String tenantURL, String apiVersion, String tenantName,
			String accessToken, PrintStream logger) throws URISyntaxException,
			UnsupportedCharsetException, ClientProtocolException, IOException, DatasetLoaderException {
		logger.println();

		List<GetBucketResponseType> bucketList = new LinkedList<GetBucketResponseType>();

		ObjectMapper mapper = new ObjectMapper();
		HttpClient httpClient = HttpUtils.getHttpClient();
		// HttpUtils.getRequestConfig();

		URI u = new URI(tenantURL);
		String tokenUriString = "/ccx/api/prismAnalytics/" + apiVersion + "/" + tenantName+ "/wBuckets/";
		URI tokenURI = new URI(u.getScheme(), u.getUserInfo(), u.getHost(), u.getPort(), tokenUriString, null, null);
		logger.println("Getting Bucket: " + tokenURI);
		HttpGet tok = new HttpGet(tokenURI);
		// tok.setConfig(requestConfig);

		tok.addHeader("Authorization", "Bearer " + accessToken);
		tok.addHeader("content-type", ContentType.APPLICATION_JSON.toString());

		HttpResponse response = httpClient.execute(tok);
		int statusCode = response.getStatusLine().getStatusCode();
		HttpEntity responseEntity = response.getEntity();
		if (responseEntity == null) {
			throw new DatasetLoaderException("Could not Get Bucket List. Error: " + response.getStatusLine().toString());
		}
		InputStream is = responseEntity.getContent();
		String responseString = IOUtils.toString(is, "UTF-8");
		// logger.println("Get Bucket Response: " + responseString);
		is.close();
		// httpClient.close();

		if (statusCode != HttpStatus.SC_OK) {
			if (responseString != null && !responseString.isEmpty()) {
				throw new DatasetLoaderException("Could not Get Bucket List. Error: " + responseString);
			} else {
				throw new DatasetLoaderException("Could not Get Bucket List. Error: " + response.getStatusLine().toString());
			}
		}

		if (responseString != null && !responseString.isEmpty()) {
			mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
			ListBucketsResponseType res = mapper.readValue(responseString, ListBucketsResponseType.class);
			if (res != null && !res.data.isEmpty()) {
				bucketList.addAll(res.data);
				/*
				 * if (pageNo == 0) { datasetCount = res.total; totalPages = datasetCount / 100;
				 * if (datasetCount % 100 > 0) totalPages = totalPages + 1; } offset = offset +
				 * 100; pageNo++; } else { break; }
				 */
			} else {
				throw new DatasetLoaderException("Failed to List Buckets: " + response.getStatusLine().toString());
			}
		}else
		{
			throw new DatasetLoaderException("Failed to List Buckets. Error: " + responseString);
		}
		return bucketList;
	}
	
	
	public static GetBucketResponseType getBucket(String tenantURL, String apiVersion, String tenantName,
			String accessToken, String bucketId, PrintStream logger) throws URISyntaxException,
			UnsupportedCharsetException, ClientProtocolException, IOException, DatasetLoaderException {
		logger.println();

		ObjectMapper mapper = new ObjectMapper();
		HttpClient httpClient = HttpUtils.getHttpClient();
		// HttpUtils.getRequestConfig();

		URI u = new URI(tenantURL);
		String tokenUriString = "/ccx/api/prismAnalytics/" + apiVersion + "/" + tenantName + "/wBuckets/" + bucketId;
		URI tokenURI = new URI(u.getScheme(), u.getUserInfo(), u.getHost(), u.getPort(), tokenUriString, null, null);
		logger.println("Getting Bucket: " + tokenURI);
		HttpGet tok = new HttpGet(tokenURI);
		// tok.setConfig(requestConfig);

		tok.addHeader("Authorization", "Bearer " + accessToken);
		tok.addHeader("content-type", ContentType.APPLICATION_JSON.toString());

		HttpResponse response = httpClient.execute(tok);
		int statusCode = response.getStatusLine().getStatusCode();
		HttpEntity responseEntity = response.getEntity();
		if (responseEntity == null) {
			throw new DatasetLoaderException("Could not Get Bucket. Error: " + response.getStatusLine().toString());
		}
		InputStream is = responseEntity.getContent();
		String responseString = IOUtils.toString(is, "UTF-8");
		// logger.println("Get Bucket Response: " + responseString);
		is.close();
		// httpClient.close();

		if (statusCode != HttpStatus.SC_OK) {
			if (responseString != null && !responseString.isEmpty()) {
				throw new DatasetLoaderException("Could not Get Bucket. Error: " + responseString);
			} else {
				throw new DatasetLoaderException("Could not Get Bucket. Error: " + response.getStatusLine().toString());
			}
		}

		if (responseString != null && !responseString.isEmpty()) {
			mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
			GetBucketResponseType res = mapper.readValue(responseString, GetBucketResponseType.class);
			if (res != null && res.getId() != null) {
				logger.println("Bucket {" + res.getId() + "} Status {" + res.getBucketState() + "}"
						+ (res.getErrorMessage() != null ? "Message {" + res.getErrorMessage() + "}" : ""));
				return res;
			}
			throw new DatasetLoaderException("Could not Get Bucket. Error: " + responseString);
		}
		throw new DatasetLoaderException("Could not Get Bucket. Error: " + response.getStatusLine().toString());
	}

	public static URI getUploadEndpoint(String tenantURL, String apiVersion, String tenantName, String accessToken,
			String bucketId, PrintStream logger)
			throws URISyntaxException, ClientProtocolException, IOException, DatasetLoaderException {
		if (bucketId == null || bucketId.trim().equalsIgnoreCase("null") || bucketId.trim().isEmpty()) {
			throw new IllegalArgumentException("bucketId cannot be blank");
		}

		URI u = new URI(tenantURL);
		HttpClient httpClient = HttpUtils.getHttpClient();

		String uploadURIString = "/wday/opa/tenant/" + tenantName + "/service/wBuckets/" + bucketId + "/files";
		/*
		if (tenantURL.contains("suv")) {
			uploadURIString = "/wday/opa/service/wBuckets/" + bucketId + "/files";
		}
		*/

		URI uploadURI = new URI(u.getScheme(), u.getUserInfo(), u.getHost(), u.getPort(), uploadURIString, null, null);

		HttpHead uploadFile = new HttpHead(uploadURI);

		uploadFile.addHeader("Authorization", "Bearer " + accessToken);

		HttpResponse response = httpClient.execute(uploadFile);

		int statusCode = response.getStatusLine().getStatusCode();
		if (statusCode == HttpStatus.SC_NOT_FOUND) {
			String altHost = u.getHost();
			int altPort = u.getPort();
			String altScheme = u.getScheme();
			altHost = u.getHost().replaceAll("-services1", "");
			uploadURI = new URI(altScheme, u.getUserInfo(), altHost, altPort, uploadURIString, null, null);
		}
		logger.println("File Upload URI: " + uploadURI);
		return uploadURI;
	}
	
	public static void shutdown()
	{
		fileUploadExecutorPool.shutdownNow();
	}

}
