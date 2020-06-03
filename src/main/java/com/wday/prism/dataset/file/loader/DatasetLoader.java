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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.MalformedInputException;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.zip.GZIPInputStream;

import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOCase;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.apache.http.client.ClientProtocolException;

import com.wday.prism.dataset.api.DataAPIConsumer;
import com.wday.prism.dataset.api.types.DatasetType;
import com.wday.prism.dataset.api.types.GetBucketResponseType;
import com.wday.prism.dataset.constants.Constants;
import com.wday.prism.dataset.file.schema.FieldType;
import com.wday.prism.dataset.file.schema.FieldTypeEnum;
import com.wday.prism.dataset.file.schema.FileSchema;
import com.wday.prism.dataset.monitor.Session;
import com.wday.prism.dataset.monitor.ThreadContext;
import com.wday.prism.dataset.util.CSVReader;
import com.wday.prism.dataset.util.CSVWriter;
import com.wday.prism.dataset.util.CharsetChecker;
import com.wday.prism.dataset.util.FileUtilsExt;
import com.wday.prism.dataset.util.StringUtilsExt;

/**
 * The Class DatasetLoader.
 */
public class DatasetLoader {

	private static final long maxWaitTime = 30 * 60 * 1000L;

	/** The Constant nf. */
	public static final NumberFormat nf = NumberFormat.getIntegerInstance();

	/** The Constant logformat. */
	static final SimpleDateFormat logformat = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss.SSS zzz");
	static {
		logformat.setTimeZone(TimeZone.getTimeZone("GMT"));
	}

	/**
	 * @param tenantURL
	 * @param apiVersion
	 * @param tenantName
	 * @param accessToken
	 * @param uploadFile
	 * @param schemaFileString
	 * @param uploadFormat
	 * @param codingErrorAction
	 * @param inputFileCharset
	 * @param datasetAlias
	 * @param datasetLabel
	 * @param operation
	 * @param logger
	 * @param createDataset
	 * @throws DatasetLoaderException
	 */
	public static boolean uploadDataset(String tenantURL, String apiVersion, String tenantName, String accessToken,
			File uploadFile, String schemaFileString, String uploadFormat, CodingErrorAction codingErrorAction,
			Charset inputFileCharset, String datasetAlias, String datasetLabel, String operation, PrintStream logger,
			boolean createDataset, boolean parseContent) throws DatasetLoaderException {
		File archiveDir = null;
		File datasetArchiveDir = null;
		File bucketDir = null;
//		File inputFile = null;
		File schemaFile = null;
		boolean status = true;
		String statusMessage = null;
		long uploadTime = 0L;

		ThreadContext tx = ThreadContext.get();
		Session session = tx.getSession();

		// we only want a small capacity otherwise the reader thread will run away
		BlockingQueue<List<String>> q = new LinkedBlockingQueue<List<String>>(10);

		if (uploadFormat == null || uploadFormat.trim().isEmpty())
			uploadFormat = "binary";

		if (codingErrorAction == null)
			codingErrorAction = CodingErrorAction.REPORT;

		if (logger == null)
			logger = System.out;
		
		if(uploadFile==null)
		{
			logger.println("Error: Upload File is null");
			throw new DatasetLoaderException("Upload File is null");	
		}
		
		if (!uploadFile.exists()) {
			logger.println("Error: File {" + uploadFile.getAbsolutePath() + "} not found");
			throw new DatasetLoaderException("File {" + uploadFile.getAbsolutePath() + "} not found");
		}

		File uploadBaseDir = uploadFile.getAbsoluteFile().getParentFile();
		String uploadFilePrefix =  FileUtilsExt.getBaseName(uploadFile);
		archiveDir = new File(uploadBaseDir, "archive");
		
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
							logger.println("Error: Input File is null");
							throw new DatasetLoaderException("Input File is null");
						}
						
						if (file.getName().toLowerCase().endsWith("__err.csv"))
							continue;
						

						if (!file.exists()) {
							logger.println("Error: File {" + file.getAbsolutePath() + "} not found");
							throw new DatasetLoaderException("File {" + file.getAbsolutePath() + "} not found");
						}

						if (file.length() == 0) {
							logger.println("Error: File {" + file.getAbsolutePath() + "} is empty");
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
				logger.println("Input File {"+uploadFile+"} does not contain any valid .csv or .csv.gz files");
				throw new DatasetLoaderException("Input File {"+uploadFile+"} does not contain any valid .csv or .csv.gz files");				
			}			
		}else
		{
			if (uploadFile.length() == 0) {
				logger.println("Error: File {" + uploadFile.getAbsolutePath() + "} is empty");
				throw new DatasetLoaderException("Error: File {" + uploadFile.getAbsolutePath() + "} is empty");
			}

			String ext = FilenameUtils.getExtension(uploadFile.getName());
			if(ext!=null && ext.equalsIgnoreCase("csv"))
			{
					if (uploadFile.length() > Constants.MAX_UNCOMPRESSED_FILE_LENGTH) {
						logger.println("Error: File {" + uploadFile.getAbsolutePath()
								+ "} size is greater than the max supported size: "
								+ Constants.MAX_UNCOMPRESSED_FILE_LENGTH / FileUtils.ONE_GB + "GB");
						throw new DatasetLoaderException("Error: File {" + uploadFile.getAbsolutePath()
								+ "} size is greater than the max supported size: "
								+ Constants.MAX_UNCOMPRESSED_FILE_LENGTH / FileUtils.ONE_GB + "GB");
					}
			} else if(ext!=null && ext.equalsIgnoreCase("gz"))
			{
					if (uploadFile.length() > Constants.MAX_COMPRESSED_FILE_LENGTH) {
						logger.println("Error: File {" + uploadFile.getAbsolutePath()
								+ "} size is greater than the max supported size: "
								+ Constants.MAX_COMPRESSED_FILE_LENGTH / FileUtils.ONE_GB + "GB");
						throw new DatasetLoaderException("Error: File {" + uploadFile.getAbsolutePath()
								+ "} size is greater than the max supported size: "
								+ Constants.MAX_COMPRESSED_FILE_LENGTH / FileUtils.ONE_GB + "GB");
					}
			}else
			{
				throw new DatasetLoaderException("Error: File {" + uploadFile.getAbsolutePath() + "} has invalid extension only .csv and .csv.gz is supported");
			}
			inputFiles.add(uploadFile);
		}
		

		if (inputFileCharset == null) {
			Charset tmp = null;
			try {
				File inputFile = inputFiles.get(0);
				if (inputFile != null && inputFile.exists() && inputFile.length() > 0) {
					tmp = CharsetChecker.detectCharset(inputFile, logger);
				}
			} catch (Exception e) {
			}

			if (tmp != null) {
				inputFileCharset = tmp;
			} else {
				inputFileCharset = Charset.forName("UTF-8");
			}
		}

		if (operation == null) {
			operation = "replace";
		}

		if (datasetLabel == null || datasetLabel.trim().isEmpty()) {
			datasetLabel = datasetAlias;
		}

		Runtime rt = Runtime.getRuntime();
		long mb = 1024 * 1024;

		logger.println("\n*******************************************************************************");
		logger.println("Start Timestamp: " + logformat.format(new Date()));
		logger.println("inputFile: " + uploadFile);
		logger.println("schemaFile: " + schemaFileString);
		logger.println("inputFileCharset: " + inputFileCharset);
		logger.println("dataset: " + datasetAlias);
		logger.println("datasetLabel: " + datasetLabel);
		logger.println("operation: " + operation);
		logger.println("uploadFormat: " + uploadFormat);
		logger.println("JVM Max memory: " + nf.format(rt.maxMemory() / mb));
		logger.println("JVM Total memory: " + nf.format(rt.totalMemory() / mb));
		logger.println("JVM Free memory: " + nf.format(rt.freeMemory() / mb));
		logger.println("*******************************************************************************\n");

		try {


			if (schemaFileString != null) {
				schemaFile = new File(schemaFileString);
				if (!schemaFile.exists()) {
					logger.println("Error: File {" + schemaFile.getAbsolutePath() + "} not found");
					throw new DatasetLoaderException("File {" + schemaFile.getAbsolutePath() + "} not found");
				}

				if (schemaFile.length() == 0) {
					logger.println("Error: File {" + schemaFile.getAbsolutePath() + "} is empty");
					throw new DatasetLoaderException("Error: File {" + schemaFile.getAbsolutePath() + "} is empty");
				}
			}

			if (datasetAlias == null || datasetAlias.trim().isEmpty()) {
				throw new DatasetLoaderException("datasetAlias cannot be null");
			}

			String santizedDatasetName = FileSchema.createDevName(datasetAlias, "Dataset", 1, false);
			if (!datasetAlias.equals(santizedDatasetName)) {
				logger.println(
						"\n Warning: dataset name can only contain alpha-numeric or '_', must start with alpha, and cannot end in '__c'");
				logger.println("\n changing dataset name to: {" + santizedDatasetName + "}");
				datasetAlias = santizedDatasetName;
			}

			if (datasetAlias.length() > 255)
				throw new DatasetLoaderException(
						"datasetName {" + datasetAlias + "} should be less than 255 characters");

			String datasetId = DatasetLoader.getDatasetId(tenantURL, apiVersion, tenantName, accessToken,
					datasetAlias, logger);


			if (session == null) {
				session = Session.getCurrentSession(tenantName, datasetAlias, false);
			}

			if (session.isDone()) {
				throw new DatasetLoaderException("operation terminated on user request");
			}

			if (schemaFile == null)
				schemaFile = FileSchema.getSchemaFile(uploadFile, logger);

			FileSchema schema = null;

//			String fileExt = FilenameUtils.getExtension(inputFile.getName());
//			boolean isParsable = false;
//			if (fileExt != null && (fileExt.equalsIgnoreCase("csv") || fileExt.equalsIgnoreCase("txt"))) {
//				isParsable = true;
//			}

//			if (!isParsable)
//				throw new DatasetLoaderException("Error: Input file should be .csv type");

			if (session.isDone()) {
				throw new DatasetLoaderException("operation terminated on user request");
			}

			if (schema == null) {
				logger.println("\n*******************************************************************************");
//				if (isParsable) {
					if (schemaFile != null && schemaFile.exists() && schemaFile.length() > 0)
						session.setStatus("LOADING SCHEMA");
					else
						session.setStatus("DETECTING SCHEMA");

					schema = FileSchema.init(inputFiles.get(0), schemaFile, inputFileCharset, logger);
					if (schema == null) {
						logger.println(
								"Failed to parse schema file {" + schemaFile + "}");
						throw new DatasetLoaderException(
								"Failed to parse schema file {" + schemaFile + "}");
					}
				}
				logger.println("*******************************************************************************\n");
//			}

			if (schema != null) {
				if ((operation.equalsIgnoreCase("upsert") || operation.equalsIgnoreCase("delete"))
						&& !FileSchema.hasUniqueID(schema)) {
					throw new DatasetLoaderException("Schema File {" + schemaFile
							+ "} must have uniqueId set for atleast one field");
				}

				if (operation.equalsIgnoreCase("append") && FileSchema.hasUniqueID(schema)) {
					throw new DatasetLoaderException("Schema File {" + schemaFile
							+ "} has a uniqueId set. Choose 'upsert' operation instead");
				}
			}

			if (session.isDone()) {
				throw new DatasetLoaderException("operation terminated on user request");
			}
			
			
			
			try {
				FileUtils.forceMkdir(archiveDir);
			} catch (Throwable t) {
				t.printStackTrace();
			}

			datasetArchiveDir = new File(archiveDir, datasetAlias);
			try {
				FileUtils.forceMkdir(datasetArchiveDir);
			} catch (Throwable t) {
				t.printStackTrace();
			}

			if (datasetId == null && createDataset == true) {
				datasetId = DataAPIConsumer.createDataset(tenantURL, apiVersion, tenantName, accessToken, datasetAlias,datasetLabel,schema,
						logger);
			}

			// Validate access to the API before going any further
			if (datasetId == null) {
				logger.println("Error: you do not have access to Prism Data API. Please contact your Workday admin");
				throw new DatasetLoaderException(
						"Error: you do not have access to Prism Data API. Please contact your Workday admin");
			}

			
			String hdrId = createBucket(tenantURL, apiVersion, tenantName, accessToken, datasetAlias, datasetId,
					datasetLabel, schema, operation, logger);

			if (hdrId == null || hdrId.isEmpty()) {
				throw new DatasetLoaderException("Error: failed to create Bucket for Dataset: " + datasetAlias);
			}

			session.setParam(Constants.hdrIdParam, hdrId);

			if (session.isDone()) {
				throw new DatasetLoaderException("operation terminated on user request");
			}

			bucketDir = new File(datasetArchiveDir, hdrId);
			try {
				FileUtils.forceMkdir(bucketDir);
			} catch (Throwable t) {
				t.printStackTrace();
			}
			
			boolean parseFile = true;
			if (!parseContent && !uploadFile.isDirectory()) {
				InputStream is = null;
				GzipCompressorOutputStream gzos = null;
				File ouputFile = null;
				long totalInputFileSize = uploadFile.length();
				long startTime = System.currentTimeMillis();

				try {

					if (FileUtilsExt.isGzipFile(uploadFile)) {
						is = new GZIPInputStream(new FileInputStream(uploadFile), Constants.DEFAULT_BUFFER_SIZE);
					} else {
						is = new FileInputStream(uploadFile);
					}

					ouputFile = new File(bucketDir, uploadFilePrefix + ".csv.gz");
					gzos = new GzipCompressorOutputStream(new FileOutputStream(ouputFile));
					IOUtils.copy(is, gzos);
					
				} catch (IOException ex) {
					ex.printStackTrace(logger);
				} finally {
					IOUtils.closeQuietly(is);
					IOUtils.closeQuietly(gzos);
				}
				
				if(ouputFile!=null && ouputFile.length()<Constants.MAX_COMPRESSED_FILE_PART_LENGTH)
				{
					parseFile = false;
					long totalOutputFileSize = ouputFile.length();
					long endTime = System.currentTimeMillis();
					long digestTime = endTime - startTime;

					logger.println("\n*******************************************************************************");
					logger.println("Succesfully created output file: " + ouputFile + "\n, % Compression: "
							+ (totalInputFileSize / totalOutputFileSize) * 100 + "%" + ", Digest Time {"
							+ nf.format(digestTime) + "} msecs");
					logger.println("*******************************************************************************\n");

				}

			}
			
			if(parseFile)
			{
				readInputFile(inputFiles,uploadBaseDir,uploadFilePrefix, bucketDir, hdrId, schema, logger, inputFileCharset, session, q, parseContent);
			}

			if (session.isDone()) {
				throw new DatasetLoaderException("operation terminated on user request");
			}

			long startTime = System.currentTimeMillis();
			DataAPIConsumer.uploadDirToBucket(tenantURL, apiVersion, tenantName, accessToken, hdrId, bucketDir, logger, session);
			long endTime = System.currentTimeMillis();
			uploadTime = endTime - startTime;

			status = DataAPIConsumer.completeBucketWithRetry(tenantURL, apiVersion, tenantName, accessToken, hdrId, logger);
			statusMessage = null;
			startTime = System.currentTimeMillis();
			while (status) {
				GetBucketResponseType serverStatus = DataAPIConsumer.getBucket(tenantURL, apiVersion, tenantName,
						accessToken, hdrId, logger);
				if (serverStatus != null) {
					session.setParam(Constants.serverStatusParam, serverStatus.getBucketState().toUpperCase());
					// Bucket state can be: New, Processing, Loading,Warning, Success, Failed
					if (serverStatus.getBucketState().equalsIgnoreCase("Success")) {
						statusMessage = serverStatus.getErrorMessage();
						break;
					} else if (serverStatus.getBucketState().equalsIgnoreCase("Warning")) {
						statusMessage = serverStatus.getErrorMessage();
						break;
					} else if (serverStatus.getBucketState().equalsIgnoreCase("Queued")) {
						Thread.sleep(5000);
						startTime = System.currentTimeMillis();
						continue;
					} else if (serverStatus.getBucketState().equalsIgnoreCase("Processing")) {
						if (System.currentTimeMillis() - startTime > maxWaitTime) {
							status = false;
							throw new DatasetLoaderException(
									"Bucket {" + hdrId + "} did not finish processing in time. Giving up...");
						}
						Thread.sleep(5000);
						continue;
					} else // status can be (New, Queued, Processing, Success, Failed) all other status are invalid states at this point
					{
						status = false;
						if (serverStatus.getErrorMessage() != null)
							throw new DatasetLoaderException(serverStatus.getErrorMessage());
						break;
					}

				}
			}

			if (session.isDone()) {
				throw new DatasetLoaderException("operation terminated on user request");
			}

		} catch (MalformedInputException mie) {
			logger.println("\n*******************************************************************************");
			logger.println("The input file is not valid utf8 encoded. Please save it as UTF8 file first");
			mie.printStackTrace(logger);
			status = false;
			logger.println("*******************************************************************************\n");
			throw new DatasetLoaderException("The input file is not utf8 encoded");
		} catch (Throwable t) {
			logger.println("\n*******************************************************************************");
			t.printStackTrace(logger);
			status = false;
			logger.println("*******************************************************************************\n");
			throw new DatasetLoaderException(t.getMessage());
		} finally {
			if (schemaFile != null && schemaFile.exists() && schemaFile.length() > 0)
				session.setParam(Constants.metadataJsonParam, schemaFile.getAbsolutePath());

			logger.println("\n*******************************************************************************");
			if (status)
				logger.println("Successfully uploaded {" + uploadFile + "} to Dataset {" + datasetAlias
						+ "} Processing Time {" + nf.format(uploadTime) + "} msecs");
			else
				logger.println("Failed to load {" + uploadFile + "} to Dataset {" + datasetAlias + "}");
			if(statusMessage!=null && !statusMessage.isEmpty())
				logger.println("Status Message: {" + statusMessage + "}");
			logger.println("*******************************************************************************\n");

			logger.println("\n*******************************************************************************");
			logger.println("End Timestamp: " + logformat.format(new Date()));
			logger.println("JVM Max memory: " + nf.format(rt.maxMemory() / mb));
			logger.println("JVM Total memory: " + nf.format(rt.totalMemory() / mb));
			logger.println("JVM Free memory: " + nf.format(rt.freeMemory() / mb));
			logger.println("*******************************************************************************\n");
		}
		return status;
	}

	/**
	 * Insert file hdr.
	 *
	 * @param partnerConnection
	 *            the partner connection
	 * @param datasetAlias
	 *            the dataset alias
	 * @param datasetContainer
	 *            the dataset container
	 * @param datasetLabel
	 *            the dataset label
	 * @param metadataJson
	 *            the metadata json
	 * @param dataFormat
	 *            the data format
	 * @param operation
	 *            the operation
	 * @param logger
	 *            the logger
	 * @param schema
	 * @return the string
	 * @throws DatasetLoaderException
	 *             the dataset loader exception
	 */
	private static String createBucket(String tenantURL, String apiVersion, String tenantName, String accessToken,
			String datasetAlias, String datasetId, String datasetLabel, FileSchema schema, String operation,
			PrintStream logger) throws DatasetLoaderException {
		String rowId = null;
		long startTime = System.currentTimeMillis();
		try {
			
			if (datasetId != null && !apiVersion.equalsIgnoreCase("v1")) {
//			if (datasetId != null) {
				
				DatasetType datasetDefinition = DataAPIConsumer.describeDataset(tenantURL, apiVersion, tenantName,
						accessToken, datasetId, logger);

				if (datasetDefinition != null && datasetDefinition.getFields() != null
						&& !datasetDefinition.getFields().isEmpty()) {

					for (FieldType field : schema.getFields()) {
						if (field == null) {
							continue;
						}

						for (FieldType other : datasetDefinition.getFields()) {
							if (other == null) {
								continue;
							}
							if (field.getName() == null) {
								continue;
							}

							if (other.getName() == null) {
								continue;
							}

							if (!field.getName().equals(other.getName())) {
								continue;
							}

							if (field.getType() == null) {
								if (other.getType() != null) {
									field.setType(other.getType());
								}
							}

							if (!field.getType().equals(other.getType())) {
								logger.println("Field: '" + field.getName() + "' type '" + field.getType()
										+ "'does not match Table type'" + other.getType() + "'");
								field.setType(other.getType());
							}

							if (field.getType() == FieldTypeEnum.NUMERIC) {
								//V1 describe return 0 precision for INT/LONG Types
								if(other.getPrecision()!=0)
								{
									if (field.getPrecision() != other.getPrecision()) {
										logger.println("Field: '" + field.getName() + "' precision '" + field.getPrecision()
												+ "'does not match table precision'" + other.getPrecision() + "'");
										field.setPrecision(other.getPrecision());
									}
								}

								if (field.getScale() != other.getScale()) {
									logger.println("Field: '" + field.getName() + "' scale '" + field.getScale()
											+ "'does not match table scale'" + other.getScale() + "'");
									field.setScale(other.getScale());
								}

								field.setParseFormat(null);
							}

							if (field.getType() == FieldTypeEnum.DATE) {
								if (field.getParseFormat() == null && other.getParseFormat() != null
										&& !other.getParseFormat().isEmpty())
									field.setParseFormat(other.getParseFormat());

								if (!field.getParseFormat().equals(other.getParseFormat())) {
									logger.println("Field: '" + field.getName() + "' parseFormat '"
											+ field.getParseFormat() + "'does not match table parseFormat'"
											+ other.getParseFormat() + "'");
									// field.setParseFormat(other.getParseFormat());
								}
							}

						}

					}
				}

			}

			rowId = DataAPIConsumer.createBucket(tenantURL, apiVersion, tenantName, accessToken, datasetAlias,
					datasetId, schema, operation, logger);

			long endTime = System.currentTimeMillis();
			if (rowId != null) {
				logger.println(
						"Created Bucket {" + rowId + "}, upload time {" + nf.format(endTime - startTime) + "} msec");
			}
		} catch (Throwable e) {
			e.printStackTrace(logger);
			throw new DatasetLoaderException("Failed to create Bucket: " + e.getMessage());
		}
		if (rowId == null)
			throw new DatasetLoaderException("Failed to create Bucket: " + datasetAlias);

		return rowId;
	}

	/**
	 * Check api access.
	 *
	 * @param accessToken
	 *            the partner connection
	 * @param logger
	 *            the logger
	 * @return true, if successful
	 * @throws DatasetLoaderException
	 */
	private static String getDatasetId(String tenantURL, String apiVersion, String tenantName, String accessToken,
			String datasetAlias, PrintStream logger) throws DatasetLoaderException {
		try {
			List<DatasetType> datasetList = DataAPIConsumer.listDatasets(tenantURL, apiVersion, tenantName, accessToken,
					null, logger);
			if (datasetList == null || datasetList.size() == 0) {
				return null;
			} else {
				for (DatasetType dataset : datasetList) {
					if (datasetAlias.equalsIgnoreCase(dataset.getName())) {
						return dataset.getId();
					}
				}
			}
		} catch (ClientProtocolException e) {
			e.printStackTrace();
		} catch (URISyntaxException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	private static void readInputFile(List<File> inputFiles,File uploadBaseDir, String uploadFilePrefix, File bucketDir, String hdrId, FileSchema schema,
			PrintStream logger, Charset inputFileCharset, Session session, BlockingQueue<List<String>> q,boolean parseContent)
			throws DatasetLoaderException, IOException {
		CSVReader reader = null;
		ErrorWriter errorWriter = null;
		WriterThread writer = null;
		// boolean status = false;
		long totalInputFileSize = 0L;
		long totalRowCount = 0L;
		long successRowCount = 0L;
		long errorRowCount = 0L;
		long startTime = System.currentTimeMillis();

		try {

			try {
				
				

				errorWriter = new ErrorWriter(uploadBaseDir, uploadFilePrefix,schema.getFields(), schema.getParseOptions().getFieldsDelimitedBy().charAt(0),
						inputFileCharset);
				
				if (session != null) {
					session.setParam(Constants.errorCsvParam, errorWriter.getErrorFile().getAbsolutePath());
				}

				writer = new WriterThread(q, uploadFilePrefix, bucketDir, schema.getFields(), errorWriter, logger, session, schema.getParseOptions().getHeaderLinesToIgnore(), parseContent);
				Thread th = new Thread(writer, "Writer-Thread");
				th.setDaemon(true);
				th.start();
				
			//Loop over the files
			for(File inputFile:inputFiles)
			{
				long rowCount = 0L;
				
				if (session != null) {
					if (session.isDone()) {
						throw new DatasetLoaderException("operation terminated on user request");
					}
				}

				InputStream is = null;
				if (FileUtilsExt.isGzipFile(inputFile)) {
					is = new GZIPInputStream(new FileInputStream(inputFile),Constants.DEFAULT_BUFFER_SIZE);
				} else {
					is = new FileInputStream(inputFile);
				}
				reader = new CSVReader(is, inputFileCharset.name(),
						new char[] { schema.getParseOptions().getFieldsDelimitedBy().charAt(0) });

				boolean hasmore = true;
				if (session != null)
					session.setStatus("DIGESTING");
				logger.println("\n*******************************************************************************");
				logger.println("File: " + inputFile + ", being digested ");
				logger.println("*******************************************************************************\n");

				List<String> row = null;
				while (hasmore) {
					if (session != null && session.isDone()) {
						throw new DatasetLoaderException("operation terminated on user request");
					}
					try {
						totalRowCount++;
						rowCount++;
						row = reader.nextRecord();
						if (row != null && !writer.isDone() && !writer.isAborted()) {
							if (rowCount <= schema.getParseOptions().getHeaderLinesToIgnore())
								continue;
							if (row.size() != 0) {
								q.put(row);
							} else {
								errorRowCount++;
							}
						} else {
							rowCount--;
							totalRowCount--;
							hasmore = false;
						}
					} catch (Exception t) {
						errorRowCount++;
						logger.println("File {"+inputFile+"} at Line {" + (rowCount) + "} has error {" + t + "}");

						if (t instanceof MalformedInputException || errorRowCount >= Constants.max_error_threshhold) {
							// status = false;
							hasmore = false;
							int retryCount = 0;
							while (!writer.isDone()) {
								retryCount++;
								try {
									Thread.sleep(1000);
									if (retryCount % 10 == 0) {
										q.put(new ArrayList<String>(0));
										logger.println("Waiting for writer to finish");
									}
								} catch (InterruptedException in) {
									in.printStackTrace();
								}
							}

							if (errorRowCount >= Constants.max_error_threshhold) {
								logger.println(
										"\n*******************************************************************************");
								logger.println("Max error threshold reached. Aborting processing");
								logger.println(
										"*******************************************************************************\n");
								throw new DatasetLoaderException("Max error threshold reached. Aborting processing");
							} else if (t instanceof MalformedInputException) {
								logger.println(
										"\n*******************************************************************************");
								logger.println("The input file is not utf8 encoded. Please save it as UTF8 file first");
								logger.println(
										"*******************************************************************************\n");
								throw new DatasetLoaderException("The input file is not utf8 encoded");
							}

						}
					} finally {
						if (session != null) {
							session.setSourceTotalRowCount(totalRowCount);
							session.setSourceErrorRowCount(errorRowCount);
						}
					}
				} // end while
				totalInputFileSize = totalInputFileSize + inputFile.length();
			} //end files 
			
			int retryCount = 0;
			while (!writer.isDone()) {
					try {
						if (retryCount % 10 == 0) {
							q.put(new ArrayList<String>(0));
							logger.println("Waiting for writer to finish");
						}
						Thread.sleep(1000);
					} catch (InterruptedException in) {
						in.printStackTrace();
					}
					retryCount++;
				}
				successRowCount = writer.getSuccessRowCount();
				errorRowCount = writer.getErrorRowCount();
			} finally {
				if (reader != null)
					reader.finalise();
			}
			long endTime = System.currentTimeMillis();
			long digestTime = endTime - startTime;

			if (writer.isAborted()) {
				throw new DatasetLoaderException("Max error threshold reached. Aborting processing");
			}

			if (successRowCount < 1) {
				logger.println("\n*******************************************************************************");
				logger.println("All rows failed. Please check {" + errorWriter.getErrorFile() + "} for error rows");
				logger.println("*******************************************************************************\n");
				throw new DatasetLoaderException(
						"All rows failed. Please check {" + errorWriter.getErrorFile() + "} for error rows");
			}
			if (errorRowCount > 1) {
				logger.println("\n*******************************************************************************");
				logger.println(nf.format(errorRowCount) + " Rows failed. Please check {"
						+ errorWriter.getErrorFile().getName() + "} for error rows");
				logger.println("*******************************************************************************\n");
			}

			if (writer.getTotalFileSize() == 0L) {
				logger.println("Error: File {" + bucketDir.getAbsolutePath() + "} not found or is zero bytes");
				throw new DatasetLoaderException(
						"Error: Output File {" + bucketDir.getAbsolutePath() + "} not found or is zero bytes");
			}

			logger.println("\n*******************************************************************************");
			logger.println("Total Rows: " + nf.format(totalRowCount - 1) + ", Success Rows: "
					+ nf.format(successRowCount) + ", Error Rows: " + nf.format(errorRowCount) + ", % Compression: "
					+ (totalInputFileSize / writer.getTotalFileSize()) * 100 + "%" + ", Digest Time {"
					+ nf.format(digestTime) + "} msecs");
			logger.println("*******************************************************************************\n");
		} finally {
		}
	}

}
