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
package com.wday.prism.dataset;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.Charset;
import java.nio.charset.CodingErrorAction;
import java.text.DecimalFormat;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.http.client.ClientProtocolException;

import com.wday.prism.dataset.api.DataAPIConsumer;
import com.wday.prism.dataset.constants.Constants;
import com.wday.prism.dataset.file.loader.DatasetLoaderException;
import com.wday.prism.dataset.file.loader.FolderUploader;
import com.wday.prism.dataset.file.schema.FileSchema;
import com.wday.prism.dataset.monitor.Session;
import com.wday.prism.dataset.util.APIEndpoint;
import com.wday.prism.dataset.util.CharsetChecker;
import com.wday.prism.dataset.util.ConsoleUtils;
import com.wday.prism.dataset.util.FileUtilsExt;
import com.wday.prism.dataset.util.StringUtilsExt;

/**
 * @author puneet.gupta
 *
 */
public class PrismDataLoaderMain {

	public static final String[][] validActions = { { "load", "Load CSV" }, { "loadAll", "Load all CSV's in a folder" },
			{ "createSchema", "create schema for CSV file" }, { "detectEncoding", "Detect file encoding" } };

	public static void main(String[] args) {

		printBanner();
		printClasspath();

		PrismDataLoaderParams params = new PrismDataLoaderParams();

		System.out.println("");
		System.out.println("Prism Data Loader called with {" + args.length + "} Params:");

		for (int i = 0; i < args.length; i++) {
			if ((i & 1) == 0) {
				System.out.print("{" + args[i] + "}");
			} else {
				if (i > 0 && (args[i - 1].equalsIgnoreCase("--p") || args[i - 1].equalsIgnoreCase("--token")))
					System.out.println(":{*******}");
				else
					System.out.println(":{" + args[i] + "}");
			}
		}
		System.out.println("");

		if (!printlneula(false)) {
			System.out.println("You do not have permission to use this software. Please delete it from this computer");
			System.exit(-1);
		}

		if (args.length == 0) {
			printUsage();
			System.exit(-1);
		}

		String action = null;

		if (args.length >= 2) {
			for (int i = 1; i < args.length; i = i + 2) {
				if (args[i - 1].equalsIgnoreCase("--help") || args[i - 1].equalsIgnoreCase("-help")
						|| args[i - 1].equalsIgnoreCase("help")) {
					printUsage();
				} else if (args[i - 1].equalsIgnoreCase("--u")) {
					params.username = args[i];
				} else if (args[i - 1].equalsIgnoreCase("--p")) {
					params.password = args[i];
				} else if (args[i - 1].equalsIgnoreCase("--token")) {
					params.token = args[i];
				} else if (args[i - 1].equalsIgnoreCase("--endpoint")) {
					try {
						params.endpoint = APIEndpoint.getAPIEndpoint(args[i]);
					} catch (Throwable t) {
						System.out.println(t.getMessage());
						System.exit(-1);
					}
				} else if (args[i - 1].equalsIgnoreCase("--action")) {
					action = args[i];
				} else if (args[i - 1].equalsIgnoreCase("--operation")) {
					if (args[i] != null) {
						if (args[i].equalsIgnoreCase("replace")) {
							params.operation = args[i].toLowerCase();
						} else if (args[i].equalsIgnoreCase("append")) {
							params.operation = args[i].toLowerCase();
							/*
							 * } else if (args[i].equalsIgnoreCase("upsert")) { params.operation = args[i];
							 * } else if (args[i].equalsIgnoreCase("delete")) { params.operation = args[i];
							 */
						} else {
							System.out.println("Invalid operation {" + args[i] + "} Must be Replace");
							System.exit(-1);
						}
					}
				} else if (args[i - 1].equalsIgnoreCase("--debug")) {
					params.debug = true;
					Constants.debug = true;
				} else if (args[i - 1].equalsIgnoreCase("--parseContent")) {
					if(args[i]!=null && args[i].equalsIgnoreCase("TRUE"))
						params.parseContent = true;
				} else if (args[i - 1].equalsIgnoreCase("--inputFile")) {
					String tmp = args[i];
					if (tmp != null) {
						File tempFile = new File(tmp);
						if (tempFile.exists()) {
							params.inputFile = tempFile.toString();
						} else {
							System.out.println("File {" + args[i] + "} does not exist");
							System.exit(-1);
						}
					}
				} else if (args[i - 1].equalsIgnoreCase("--schemaFile")) {
					String tmp = args[i];
					if (tmp != null) {
						File tempFile = new File(tmp);
						if (tempFile.exists()) {
							params.schemaFile = tempFile.toString();
						} else {
							System.out.println("File {" + args[i] + "} does not exist");
							System.exit(-1);
						}
					}
				} else if (args[i - 1].equalsIgnoreCase("--dataset")) {
					params.dataset = args[i];
				} else if (args[i - 1].equalsIgnoreCase("--datasetLabel")) {
					params.datasetLabel = args[i];
				} else if (args[i - 1].equalsIgnoreCase("--fileEncoding")) {
					if (args[i] != null && !args[i].trim().isEmpty()) {
						try {
							params.fileEncoding = Charset.forName(args[i]);
						} catch (Throwable e) {
							// e.printStackTrace();
							System.out.println("\nERROR: Invalid fileEncoding {" + params.fileEncoding + "}");
							System.exit(-1);
						}
					}
				} else if (args[i - 1].equalsIgnoreCase("--codingErrorAction")) {
					if (args[i] != null) {
						if (args[i].equalsIgnoreCase("IGNORE")) {
							params.codingErrorAction = CodingErrorAction.IGNORE;
						} else if (args[i].equalsIgnoreCase("REPORT")) {
							params.codingErrorAction = CodingErrorAction.REPORT;
						} else if (args[i].equalsIgnoreCase("REPLACE")) {
							params.codingErrorAction = CodingErrorAction.REPLACE;
						}
						Constants.codingErrorAction = params.codingErrorAction;
					}
				} else if (args[i - 1].equalsIgnoreCase("--tablePrefix")) {
					if (args[i] != null && !args[i].trim().isEmpty()) {
						params.tablePrefix = FileSchema.createDevName(args[i], "Dataset", 1, false);
					}else {
						params.tablePrefix = null;
					}
				} else {
					printUsage();
					System.out.println("\nERROR: Invalid argument: " + args[i - 1]);
					System.exit(-1);
				}
			} // end for
		}

		if (args.length == 0 || action == null) {
			while (true) {
				action = getActionFromUser();
				if (action == null || action.isEmpty()) {
					System.exit(-1);
				}
				params = new PrismDataLoaderParams();
				getRequiredParams(action, params);
				@SuppressWarnings("unused")
				boolean status = doAction(action, params);
			}
		} else {
			boolean status = doAction(action, params);
			if (!status) {
				System.exit(-1);
			}
		}
	}

	public static void printUsage() {
		System.out.println("\n*******************************************************************************");
		System.out.println(
				"Usage: java -jar workday-prism-analytics-data-loader-<version>.jar --action load --endpoint WokdayRestAPIEndpoint --u clientId --p clientSecret --dataset datasetName --inputFile inputFile");
		System.out.println("");
		System.out.println(
				"--action  : load, loadAll, createSchema, detectEncoding. Use load for loading file/directory, loadall for loading all files in a folder to different datasets based on file name");
		System.out.println("--u       : Workday Rest API Client Id");
		System.out.println("--p       : Workday Rest API Client Secret");
		System.out.println("--token   : Workday Rest API Refresh Token");
		System.out.println(
				"--endpoint: The Workday Rest API Endpoint URL. Example: https://wd2-impl-services1.workday.com/ccx/api/v1/{tenantName}");
		System.out.println("--inputFile : The input csv file or directory");
		System.out.println("--dataset : (Optional) the dataset name (required if action=load)");
		System.out.println("--fileEncoding : (Optional) the encoding of the inputFile default UTF-8");
		System.out.println("*******************************************************************************\n");
		System.out.println("");
		System.out.println("Example 1: Upload a file/directory to a dataset");
		System.out.println(
				"java -jar workday-prism-analytics-data-loader-<version>.jar --action load --endpoint https://wd2-impl-services1.workday.com/ccx/api/v1/{tenantName} --u 12345#! --p @#@#@# --token A1B2C3A1B2C3A1B2C3A1B2C3 --inputFile Worker.csv --dataset test");
		System.out.println("");
		System.out.println("Example 2: Upload all files in a folder to prism");
		System.out.println(
				"java -jar workday-prism-analytics-data-loader-<version>.jar --action loadAll --endpoint https://wd2-impl-services1.workday.com/ccx/api/v1/{tenantName} --u 12345#! --p @#@#@# --token A1B2C3A1B2C3A1B2C3A1B2C3 --inputFile dataDirectory --tablePrefix Table_");
		System.out.println("");
		System.out.println("Example 3: Generate the schema file from CSV");
		System.out.println(
				"java -jar workday-prism-analytics-data-loader-<version>.jar --action createSchema --inputFile Worker.csv");
		System.out.println("");
		System.out.println("Example 4: detect file encoding");
		System.out.println(
				"java -jar workday-prism-analytics-data-loader-<version>.jar --action detectEncoding --inputFile Worker.csv");
		System.out.println("");
	}

	static boolean printlneula(boolean server) {
		try {
			String userHome = System.getProperty("user.home");
			File lic = new File(userHome, ".wday.prismdataloader.lic");
			if (!lic.exists()) {
				if (!server) {
					System.out.println(eula);
					System.out.println();
					while (true) {
						String response = ConsoleUtils
								.readInputFromConsole("Do you agree to the above license agreement (Yes/No): ");
						if (response != null && (response.equalsIgnoreCase("yes") || response.equalsIgnoreCase("y"))) {
							FileUtils.writeStringToFile(lic, eula);
							return true;
						} else if (response != null
								&& (response.equalsIgnoreCase("no") || response.equalsIgnoreCase("n"))) {
							return false;
						}
					}
				} else {
					FileUtils.writeStringToFile(lic, eula);
					return true;
				}
			} else {
				return true;
			}
		} catch (Throwable t) {
			t.printStackTrace();
		}
		return false;
	}

	public static String eula = "\n"
			+ " ===============================================================================\n"
			+ " Copyright (c) 2018 Workday, Inc.\n" + " \n"
			+ " Licensed under the Apache License, Version 2.0 (the \"License\"); you may not \n"
			+ " use this file except in compliance with the License. You may obtain a copy of \n"
			+ " the License at: \n" + " http://www.apache.org/licenses/LICENSE-2.0 \n" + " \n"
			+ " Unless required by applicable law or agreed to in writing, software distributed\n"
			+ " under the License is distributed on an \"AS IS\" BASIS, WITHOUT WARRANTIES OR  \n"
			+ " CONDITIONS OF ANY KIND, either express or implied. See the License for the \n"
			+ " specific language governing permissions and limitations under the License.\n"
			+ " ===============================================================================\n";

	public static String getInputFromUser(String message, boolean isRequired, boolean isPassword) {
		String input = null;
		while (true) {
			try {
				if (!isPassword)
					input = ConsoleUtils.readInputFromConsole(message);
				else
					input = ConsoleUtils.readPasswordFromConsole(message);

				if (input == null || input.isEmpty()) {
					if (!isRequired)
						break;
				} else {
					break;
				}
			} catch (Throwable me) {
				input = null;
			}
		}
		return input;
	}

	public static String getActionFromUser() {
		System.out.println();
		String selectedAction = "load";
		DecimalFormat df = new DecimalFormat("00");
		df.setMinimumIntegerDigits(2);
		int cnt = 1;
		for (String[] action : validActions) {
			if (cnt == 1)
				System.out.println("Available Workday Prism Analytics Data Loader Actions: ");
			System.out.print(" ");
			System.out.println(StringUtilsExt.padLeft(cnt + "", 3) + ". " + action[1]);
			cnt++;
		}
		System.out.println();

		while (true) {
			try {
				String tmp = ConsoleUtils.readInputFromConsole("Enter Action number (0  = Exit): ");
				if (tmp == null)
					return null;
				if (tmp.trim().isEmpty())
					continue;
				long choice = Long.parseLong(tmp.trim());
				if (choice == 0)
					return null;
				cnt = 1;
				if (choice > 0 && choice <= validActions.length) {
					for (String[] action : validActions) {
						if (cnt == choice) {
							selectedAction = action[0];
							return selectedAction;
						}
						cnt++;
					}
				}
			} catch (Throwable me) {
			}
		}
	}

	public static boolean doAction(String action, PrismDataLoaderParams params) {

		if (action == null) {
			printUsage();
			System.out.println("\nERROR: Invalid action {" + action + "}");
			return false;
		}

		if (params.inputFile != null) {
			File tempFile = validateInputFile(params.inputFile, action);
			if (tempFile == null) {
				System.out.println("Inputfile {" + params.inputFile + "} is not valid");
				return false;
			}
		}

		if (action.equalsIgnoreCase("loadall") || (action.equalsIgnoreCase("load"))) {
			getTenantCredentials(params);

			if (params.dataset != null && !params.dataset.isEmpty()) {
				if (params.datasetLabel == null)
					params.datasetLabel = params.dataset;
				String santizedDatasetName = FileSchema.createDevName(params.dataset, "Dataset", 1, false);
				if (!params.dataset.equals(santizedDatasetName)) {
					System.out.println(
							"\n Warning: dataset name can only contain alpha-numeric or '_', must start with alpha, and cannot end in '__c'");
					System.out.println("\n changing dataset name to: {" + santizedDatasetName + "}");
					params.dataset = santizedDatasetName;
				}
				
				if(action.equalsIgnoreCase("loadall") && params.tablePrefix!=null)
				{
					params.dataset = params.tablePrefix + "_" +	params.dataset;	
				}
			}
		}

		if (action.equalsIgnoreCase("loadall")) {
			if (params.inputFile == null || params.inputFile.isEmpty()) {
				System.out.println("\nERROR: inputFile must be specified");
				return false;
			}

			try {
				try {
					boolean status = FolderUploader.uploadFolder(params.endpoint.tenantURL, params.endpoint.apiVersion,
							params.endpoint.tenant, params.accessToken, params.inputFile, params.schemaFile, "CSV",
							params.codingErrorAction, params.fileEncoding, params.dataset,params.tablePrefix ,params.datasetLabel,
							params.operation, System.out, true);
					return status;
				} catch (DatasetLoaderException e) {
					return false;
				}finally
				{
					DataAPIConsumer.shutdown();
				}
			} catch (Exception e) {
				System.out.println();
				e.printStackTrace(System.out);
				return false;
			}
		} else if (action.equalsIgnoreCase("load")) {
			if (params.inputFile == null || params.inputFile.isEmpty()) {
				System.out.println("\nERROR: inputFile must be specified");
				return false;
			}

			if (params.dataset == null || params.dataset.isEmpty()) {
				System.out.println("\nERROR: dataset name must be specified");
				return false;
			}

			Session session = null;
			try {
				String orgId = params.endpoint.tenant;
				session = Session.getCurrentSession(orgId, params.dataset, true);
				session.start();
				try {
					boolean status = com.wday.prism.dataset.file.loader.DatasetLoader.uploadDataset(
							params.endpoint.tenantURL, params.endpoint.apiVersion, params.endpoint.tenant,
							params.accessToken, new File(params.inputFile), params.schemaFile, "CSV", params.codingErrorAction,
							params.fileEncoding, params.dataset, params.datasetLabel, params.operation, System.out,
							true, params.parseContent);
					if (status)
						session.end();
					else
						session.fail("Check sessionLog for details");
					return status;
				} catch (DatasetLoaderException e) {
					e.printStackTrace(System.out);
					session.fail(e.getMessage());
					return false;
				}finally
				{
					DataAPIConsumer.shutdown();
				}
			} catch (Exception e) {
				System.out.println();
				e.printStackTrace(System.out);
				if (session != null)
					session.fail(e.getLocalizedMessage());
				return false;
			}
		} else if (action.equalsIgnoreCase("detectEncoding")) {
			if (params.inputFile == null) {
				System.out.println("\nERROR: inputFile must be specified");
				return false;
			}

			try {
				CharsetChecker.detectCharset(new File(params.inputFile), System.out);
			} catch (Exception e) {
				e.printStackTrace(System.out);
				return false;
			}
		} else if (action.equalsIgnoreCase("createSchema")) {
			if (params.inputFile == null) {
				System.out.println("\nERROR: inputFile must be specified");
				return false;
			}

			try {
				FileSchema emd = FileSchema.createAutoSchema(new File(params.inputFile), null, params.fileEncoding,
						System.out);
				FileSchema.save(new File(params.inputFile), emd, System.out);
			} catch (Throwable t) {
				t.printStackTrace();
				return false;
			}

		} else {
			printUsage();
			System.out.println("\nERROR: Invalid action {" + action + "}");
			return false;
		}
		return true;
	}

	public static void getTenantCredentials(PrismDataLoaderParams params) {
		if (params.username == null || params.username.trim().isEmpty()) {
			params.username = getInputFromUser("Enter Workday Rest API Client Id: ", true, false);
		}

		if (params.password == null || params.password.trim().isEmpty()) {
			params.password = getInputFromUser("Enter Workday Rest API Client Secret: ", true, false);
		}

		if (params.token == null || params.token.trim().isEmpty()) {
			params.token = getInputFromUser("Enter Workday Rest API Refresh Token: ", true, false);
		}

		while (params.endpoint == null) {
			String temp = getInputFromUser("Enter Workday Rest API Endpoint: ", true, false);
			if (temp == null || temp.trim().isEmpty())
				System.out.println("\nERROR: endpoint must be specified");

			try {
				params.endpoint = APIEndpoint.getAPIEndpoint(temp);
			} catch (Throwable t) {
				System.out.println(t.getMessage());
			}
		}

		try {
			params.accessToken = getTenantCredentials(params.endpoint, params.username, params.password, params.token);
		} catch (Throwable e) {
			e.printStackTrace();
			System.out.println("\nERROR: Invalid credentials for workday tenant");
			System.exit(-1);
		}

	}
	
	public static String getTenantCredentials(APIEndpoint endpoint,  String clientId,
			String clientSecret, String refreshToken) throws ClientProtocolException, IOException, URISyntaxException, DatasetLoaderException {
			
		if (endpoint == null) {
			throw new DatasetLoaderException("Invalid APIEndpoint: "+endpoint);
		}

		if (clientId == null || clientId.trim().isEmpty()) {
			throw new DatasetLoaderException("Invalid clientId: "+clientId);
		}

		if (clientSecret == null || clientSecret.trim().isEmpty()) {
			throw new DatasetLoaderException("Invalid clientSecret: "+clientSecret);
		}

		if (refreshToken == null || refreshToken.isEmpty()) {
			throw new DatasetLoaderException("Invalid refreshToken: "+refreshToken);
		}
			return DataAPIConsumer.getAccessToken(endpoint.tenantURL, endpoint.apiVersion,
					endpoint.tenant, clientId, clientSecret, refreshToken, System.out);
	}


	public static void getRequiredParams(String action, PrismDataLoaderParams params) {
		if (action == null || action.trim().isEmpty()) {
			System.out.println("\nERROR: Invalid action {" + action + "}");
			System.out.println();
			return;
		} else if (action.equalsIgnoreCase("load") || action.equalsIgnoreCase("loadAll")) {
			while (params.inputFile == null || params.inputFile.isEmpty()) {
				String tmp = getInputFromUser("Enter inputFile: ", true, false);
				if (tmp != null) {
					File tempFile = validateInputFile(tmp, action);
					if (tempFile != null) {
						params.inputFile = tempFile.toString();
						break;
					}
				} else
					System.out.println("File {" + tmp + "} not found");
				System.out.println();
			}

			if (action.equalsIgnoreCase("load") && (params.dataset == null || params.dataset.isEmpty())) {
				params.dataset = getInputFromUser("Enter dataset name: ", true, false);
			}

			while (params.operation == null || params.operation.isEmpty()) {
				params.operation = getInputFromUser("Enter operation (Default=Replace): ", false, false);
				if (params.operation == null || params.operation.isEmpty()) {
					params.operation = "overwrite";
				} else {
					if (params.operation.equalsIgnoreCase("replace")) {
						params.operation = "overwrite";
					} else {
						System.out.println("Invalid operation {" + params.operation + "} Must be Replace");
						params.operation = null;
					}
				}

			}

			if (params.fileEncoding == null) {
				while (true) {
					String temp = getInputFromUser("Enter fileEncoding (Optional): ", false, false);
					if (temp != null && !temp.trim().isEmpty()) {
						try {
							params.fileEncoding = Charset.forName(temp);
							break;
						} catch (Throwable e) {
						}
					} else {
						params.fileEncoding = null;
						break;
					}
					System.out.println("\nERROR: Invalid fileEncoding {" + temp + "}");
					System.out.println();
				}
			}

		} else if (action.equalsIgnoreCase("detectEncoding")) {
			while (params.inputFile == null || params.inputFile.isEmpty()) {
				String tmp = getInputFromUser("Enter inputFile: ", true, false);
				if (tmp != null) {
					File tempFile = validateInputFile(tmp, action);
					if (tempFile != null) {
						params.inputFile = tempFile.toString();
						break;
					}
				} else
					System.out.println("File {" + tmp + "} not found");
				System.out.println();
			}
		} else if (action.equalsIgnoreCase("createSchema")) {
			while (params.inputFile == null || params.inputFile.isEmpty()) {
				String tmp = getInputFromUser("Enter inputFile: ", true, false);
				if (tmp != null) {
					File tempFile = validateInputFile(tmp, action);
					if (tempFile != null) {
						params.inputFile = tempFile.toString();
						break;
					}
				} else
					System.out.println("File {" + tmp + "} not found");
				System.out.println();
			}
		} else {
			printUsage();
			System.out.println("\nERROR: Invalid action {" + action + "}");
		}
	}

	public static File validateInputFile(String inputFile, String action) {
		File temp = null;
		if (inputFile != null) {
			temp = new File(inputFile);
			temp = temp.getAbsoluteFile();

			if (!temp.exists() && !temp.canRead()) {
				System.out.println("\nERROR: inputFile {" + temp + "} not found");
				return null;
			}

			if (action.equalsIgnoreCase("loadall")) {
				if (!temp.isDirectory()) {
					System.out.println("\nERROR: inputFile {" + temp + "} is not a directory");
					return null;
				}
				return temp;
			}

			if(!temp.isDirectory())
			{
				String ext = FilenameUtils.getExtension(temp.getName());
				if (ext == null || !(ext.equalsIgnoreCase("csv") || (ext.equalsIgnoreCase("gz")))) {
					System.out.println("\nERROR: inputFile does not have valid extension, only csv files are supported");
					return null;
				}

				if (action.equalsIgnoreCase("load")) {
					if (ext.equalsIgnoreCase("gz")) {
						try {
							if (!FileUtilsExt.isGzipFile(temp)) {
								System.out.println("\nERROR: inputFile is not valid gzip file");
								return null;
							}
						} catch (FileNotFoundException e) {
							e.printStackTrace();
							System.out.println("\nERROR: inputFile {" + temp + "} not found");
							return null;
						} catch (IOException e) {
							e.printStackTrace();
							System.out.println("\nERROR: inputFile {" + temp + "} in not valid");
							return null;
						}
	
					}
				}
			}

		}
		return temp;

	}

	public static void printBanner() {
		for (int i = 0; i < 5; i++)
			System.out.println();
		System.out.println("\n\t\t**************************************************");
		System.out.println("\t\tWorkday Prism Analytics Data Loader - " + getAppversion());
		System.out.println("\t\t**************************************************\n");
	}

	public static String getAppversion() {
		try {
			Properties versionProps = new Properties();
			versionProps.load(PrismDataLoaderMain.class.getClassLoader().getResourceAsStream("tablePrefix.properties"));
			return versionProps.getProperty("workday-prism-analytics-data-loader.version");
		} catch (Throwable t) {
			// t.printStackTrace();
		}
		return "0.0.0";
	}

	public static void printClasspath() {
		System.out.println("\n*******************************************************************************");
		System.out.println("java.version:" + System.getProperty("java.version"));
		System.out.println("java.class.path:" + System.getProperty("java.class.path"));
		System.out.print("SystemClassLoader:");
		ClassLoader sysClassLoader = ClassLoader.getSystemClassLoader();
		URL[] urls = ((URLClassLoader) sysClassLoader).getURLs();
		for (int i = 0; i < urls.length; i++) {
			System.out.println(urls[i].getFile());
		}
		System.out.println("*******************************************************************************\n");
	}

}
