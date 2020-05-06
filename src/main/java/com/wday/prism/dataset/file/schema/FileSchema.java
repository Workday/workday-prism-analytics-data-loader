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
package com.wday.prism.dataset.file.schema;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.input.BOMInputStream;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.wday.prism.dataset.file.loader.DatasetLoaderException;
import com.wday.prism.dataset.util.CSVReader;
import com.wday.prism.dataset.util.CharsetChecker;
import com.wday.prism.dataset.util.FileUtilsExt;
import com.wday.prism.dataset.util.SeparatorGuesser;
import com.wday.prism.dataset.util.StringUtilsExt;

public class FileSchema {

	private static final String SCHEMA_FILE_SUFFIX = "_schema.json";

	private ParseOptions parseOptions;
	private List<FieldType> fields;
	private Map<String, String> schemaVersion = new LinkedHashMap<String, String>();

	public FileSchema() {
		super();
	}

	public FileSchema(FileSchema old) {
		super();
		if (old != null) {
			this.parseOptions = new ParseOptions(old.parseOptions);
			this.fields = new LinkedList<FieldType>();
			if (old.fields != null) {
				for (FieldType obj : old.fields) {
					this.fields.add(new FieldType(obj));
				}
			}
		}
	}

	public ParseOptions getParseOptions() {
		if (parseOptions == null) {
			parseOptions = new ParseOptions();
		}
		return parseOptions;
	}

	public void setParseOptions(ParseOptions parseOptions) {
		if (parseOptions == null) {
			parseOptions = new ParseOptions();
		}
		this.parseOptions = parseOptions;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((parseOptions == null) ? 0 : parseOptions.hashCode());
		result = prime * result + ((fields == null) ? 0 : fields.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		FileSchema other = (FileSchema) obj;
		if (parseOptions == null) {
			if (other.parseOptions != null) {
				return false;
			}
		} else if (!parseOptions.equals(other.parseOptions)) {
			return false;
		}
		if (fields == null) {
			if (other.fields != null) {
				return false;
			}
		} else if (!fields.equals(other.fields)) {
			return false;
		}
		return true;
	}

	public static FileSchema init(File csvFile, File schemaFile, Charset fileCharset, PrintStream logger)
			throws JsonParseException, JsonMappingException, IOException, DatasetLoaderException {
		FileSchema newSchema = null;

		if (fileCharset == null) {
			Charset tmp = null;
			if (csvFile.exists() && csvFile.length() > 0) {
				tmp = CharsetChecker.detectCharset(csvFile, logger);
			}
			if (tmp != null) {
				fileCharset = tmp;
			} else {
				fileCharset = Charset.forName("UTF-8");
			}
		}

		FileSchema userSchema = FileSchema.load(csvFile, schemaFile, fileCharset, logger);
		FileSchema autoSchema = null;
		if (userSchema == null || userSchema.getParseOptions().getHeaderLinesToIgnore()!=0)
		{
		 autoSchema = FileSchema.createAutoSchema(csvFile, userSchema, fileCharset, logger);
		}

		if (autoSchema == null && userSchema != null) {
			autoSchema = userSchema;
		}

		if (userSchema == null && autoSchema != null) {
			FileSchema.save(schemaFile!=null?schemaFile:csvFile, autoSchema, logger);
			userSchema = autoSchema;
		}

		if (userSchema != null && !userSchema.equals(autoSchema)) {
			FileSchema schema = FileSchema.merge(userSchema, autoSchema, logger);
			if (!schema.equals(userSchema)) {
				logger.println("Saving merged schema");
				FileSchema.save(schemaFile!=null?schemaFile:csvFile, schema, logger);
			}
			newSchema = schema;
		} else {
			newSchema = autoSchema;
		}
		validateSchema(newSchema, logger);
		return newSchema;
	}

	public static FileSchema createAutoSchema(File csvFile, FileSchema userSchema, Charset fileCharset,
			PrintStream logger) throws IOException, DatasetLoaderException {
		FileSchema emd = null;
		String fileExt = FilenameUtils.getExtension(csvFile.getName());

		boolean isParsable = false;
		if (fileExt != null && (fileExt.equalsIgnoreCase("csv") || fileExt.equalsIgnoreCase("txt")
				|| csvFile.getName().toLowerCase().endsWith(".csv.gz"))) {
			isParsable = true;
		}

		if (!isParsable) {
			if (userSchema == null) {
				throw new DatasetLoaderException("Failed to determine schema for file {" + csvFile
						+ "} as its  not parsable. File must have an extension of .csv or .txt");
			} else
				return null;
		}

		char delim = ',';
		if (userSchema != null) {
			delim = userSchema.getParseOptions().getFieldsDelimitedBy().charAt(0);
		} else if (fileExt == null
				|| !(fileExt.equalsIgnoreCase("csv") || csvFile.getName().toLowerCase().endsWith(".csv.gz"))) {
			delim = SeparatorGuesser.guessSeparator(csvFile, fileCharset, true);
			// logger.println("\n*******************************************************************************");
			// logger.println("File {"+csvFile+"} has delimiter {"+delim+"}");
			// logger.println("*******************************************************************************\n");

			if (delim == 0) {
				// logger.println("Failed to determine field Delimiter for file {"+csvFile+"}");
				// delim = ',';
				throw new DatasetLoaderException("Failed to determine field Delimiter for file {" + csvFile + "}");
			} else {
				logger.println("File {" + csvFile + "} has delimiter {" + delim + "}");
			}
		}

		DetectFieldTypes detEFT = new DetectFieldTypes();
		List<FieldType> fields = detEFT.detect(csvFile, userSchema, fileCharset, delim, logger);
		ParseOptions parseOptions = new ParseOptions();
		parseOptions.setFieldsDelimitedBy(delim + "");

		emd = new FileSchema();
		emd.parseOptions = parseOptions;
		emd.fields = fields;

		validateSchema(emd, logger);
		return emd;
	}

	public static FileSchema createAutoSchema(InputStream csvFile, char delim, Charset fileCharset, PrintStream logger)
			throws IOException, DatasetLoaderException {
		FileSchema emd = null;
		DetectFieldTypes detEFT = new DetectFieldTypes();
		List<FieldType> fields = detEFT.detect(csvFile, null, fileCharset, delim, logger);
		ParseOptions parseOptions = new ParseOptions();
		parseOptions.setFieldsDelimitedBy(delim + "");

		emd = new FileSchema();
		emd.parseOptions = parseOptions;
		emd.fields = fields;

		validateSchema(emd, logger);
		return emd;
	}

	public static void save(File schemaFile, FileSchema emd, PrintStream logger) {
		ObjectMapper mapper = new ObjectMapper();
		mapper.setSerializationInclusion(Include.NON_NULL);
		try {
			if (!schemaFile.getName().toLowerCase().endsWith(SCHEMA_FILE_SUFFIX)) {
				FilenameUtils.getBaseName(schemaFile.getName());
				schemaFile = new File(schemaFile.getParent(),
						FilenameUtils.getBaseName(schemaFile.getName()) + SCHEMA_FILE_SUFFIX);
			}
			mapper.writerWithDefaultPrettyPrinter().writeValue(schemaFile, emd);
			logger.println("Schema saved to file {" + schemaFile + "}");
		} catch (Throwable t) {
			t.printStackTrace(logger);
		}
	}

	public static FileSchema load(File schemaFile, Charset fileCharset, PrintStream logger)
			throws JsonParseException, JsonMappingException, IOException {
		ObjectMapper mapper = new ObjectMapper();
		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

		if (fileCharset == null)
			fileCharset = Charset.forName("UTF-8");

		FileSchema userSchema = null;
		if (schemaFile != null && schemaFile.exists()) {
			logger.println("Loading existing schema from file {" + schemaFile + "}");
			InputStreamReader reader = new InputStreamReader(new BOMInputStream(new FileInputStream(schemaFile), false),
					StringUtilsExt.utf8Decoder(null, Charset.forName("UTF-8")));
			userSchema = mapper.readValue(reader, FileSchema.class);
		}

		if (userSchema == null)
			return null;

		validateSchema(userSchema, logger);

		return userSchema;
	}

	public static FileSchema load(File inputCSV, File schemaFile, Charset fileCharset, PrintStream logger)
			throws JsonParseException, JsonMappingException, IOException {

		String fileExt = FilenameUtils.getExtension(inputCSV.getName());

		ObjectMapper mapper = new ObjectMapper();
		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

		if (fileCharset == null)
			fileCharset = Charset.forName("UTF-8");

		if (schemaFile == null) {
			if (!inputCSV.getName().toLowerCase().endsWith(SCHEMA_FILE_SUFFIX)) {
				String temp = inputCSV.getName();
				if (temp.endsWith(".gz")) {
					temp = temp.substring(0, temp.length() - 3);
				}
				schemaFile = new File(inputCSV.getParent(), FilenameUtils.getBaseName(temp) + SCHEMA_FILE_SUFFIX);
			} else {
				schemaFile = inputCSV;
			}
		}

		FileSchema userSchema = null;
		if (schemaFile.exists()) {
			logger.println("Loading existing schema from file {" + schemaFile + "}");
			InputStreamReader reader = new InputStreamReader(new BOMInputStream(new FileInputStream(schemaFile), false),
					StringUtilsExt.utf8Decoder(null, Charset.forName("UTF-8")));
			userSchema = mapper.readValue(reader, FileSchema.class);
		}

		if (userSchema == null)
			return null;

		validateSchema(userSchema, logger);

		boolean isParsable = false;
		if (fileExt != null && (fileExt.equalsIgnoreCase("csv") || fileExt.equalsIgnoreCase("txt"))) {
			isParsable = true;
		}

		if (isParsable && userSchema != null && userSchema.getParseOptions() != null
				&& userSchema.getParseOptions().getHeaderLinesToIgnore() == 1) {
			ArrayList<String> header = null;
			CSVReader reader = null;
			// CsvListReader reader = null;
			// CsvPreference pref = new CsvPreference.Builder((char)
			// CsvPreference.STANDARD_PREFERENCE.getQuoteChar(),
			// userSchema.getParseOptions().getFieldsDelimitedBy().charAt(0),
			// CsvPreference.STANDARD_PREFERENCE.getEndOfLineSymbols()).build();

			try {
				reader = new CSVReader(new FileInputStream(inputCSV), fileCharset.name(),
						new char[] { userSchema.getParseOptions().getFieldsDelimitedBy().charAt(0) });
				// reader = new CsvListReader(new InputStreamReader(new BOMInputStream(new
				// FileInputStream(inputCSV), false), ConsoleUtils.utf8Decoder(null ,
				// fileCharset)), pref);
				header = reader.nextRecord();
			} catch (Throwable t) {
				t.printStackTrace();
			} finally {
				if (reader != null) {
					try {
						reader.finalise();
						reader = null;
					} catch (Throwable e) {
					}
				}
			}

			int SchemaFieldCount = 0;
			List<FieldType> fields = userSchema.fields;
			if (fields != null && !fields.isEmpty()) {
				for (FieldType field : fields) {
					SchemaFieldCount++;
				}
			}

			if (header != null && header.size() > 0 && header.size() != SchemaFieldCount) {
				throw new IllegalArgumentException("CSV header count [" + header.size()
						+ "] does not match JSON Field count [" + SchemaFieldCount + "]");
			}
		}
		return userSchema;
	}

	public List<FieldType> getFields() {
		return fields;
	}

	public void setFields(List<FieldType> fields) {
		this.fields = fields;
	}

	public Map<String, String> getSchemaVersion() {
		if (schemaVersion != null && !schemaVersion.containsKey("id")) {
			schemaVersion.put("id", "Schema_Version=1.0");
		}
		return schemaVersion;
	}

	public void setSchemaVersion(Map<String, String> schemaVersion) {
		this.schemaVersion = schemaVersion;
	}

	private static void validateSchema(FileSchema schema, PrintStream logger) throws IllegalArgumentException {
		StringBuffer message = new StringBuffer();
		if (schema != null) {
			List<FieldType> user_fields = schema.getFields();

			if (user_fields != null && !user_fields.isEmpty() && user_fields.size() <= 5000) {
				HashSet<String> fieldNames = new HashSet<String>();
				HashSet<String> uniqueIdfieldNames = new HashSet<String>();
				int fieldCount = 0;
				for (FieldType user_field : user_fields) {
					fieldCount++;
					if (user_field != null) {
						if (user_field != null && user_field.getName() != null && !user_field.getName().isEmpty()) {
							if (user_field.getName().length() > 255) {
								message.append(
										"field name {" + user_field.getName() + "} is greater than 255 characters\n");
							}

							if (!createDevName(user_field.getName(), "Column", (fieldCount - 1), true)
									.equals(user_field.getName())) {
								message.append(
										"field name {" + user_field.getName() + "} contains invalid characters \n");
							}

							if (!fieldNames.add(user_field.getName().toUpperCase())) {
								message.append("Duplicate field name {" + user_field.getName() + "}\n");
							}

						} else {
							message.append("fields[" + fieldCount + "].name] in schema cannot be null or empty\n");
						}

						if (user_field.getType() == FieldTypeEnum.NUMERIC) {

							if (!(user_field.getPrecision() > 0
									&& user_field.getPrecision() <= FieldType.max_precision)) {
								message.append("field {" + user_field.getName()
										+ "}  in schema must have precision between (>0 && <" + FieldType.max_precision
										+ ")\n");
							}

							if (user_field.getPrecision() > 0 && user_field.getScale() >= user_field.getPrecision()) {
								message.append("field {" + user_field.getName()
										+ "}  in schema must have scale less than the precision\n");
							}

							if (user_field.getDefaultValue() != null) {
								try {
									new BigDecimal(user_field.getDefaultValue());
								} catch (Throwable t) {
									message.append("field {" + user_field.getName()
											+ "}  in schema has invalid defaultValue\n");
								}
							}

							if (user_field.getParseFormat() != null && user_field.getCompiledNumberFormat() == null) {
								message.append(
										"field {" + user_field.getName() + "}  in schema has invalid parse format {"
												+ user_field.getParseFormat() + "}\n");
							}
						} else if (user_field.getType() == FieldTypeEnum.BOOLEAN) {
							if (user_field.getDefaultValue() != null) {
								if (!Boolean.TRUE.toString().equalsIgnoreCase(user_field.getDefaultValue())
										&& !Boolean.FALSE.toString().equalsIgnoreCase(user_field.getDefaultValue())) {
									message.append("field {" + user_field.getName()
											+ "}  in schema has invalid defaultValue\n");
								}
							}
						} else if (user_field.getType() == FieldTypeEnum.TEXT) {
							if (user_field.getPrecision() > 32000) {
								message.append("field {" + user_field.getName()
										+ "}  in schema must have precision between (>0 && <32,000)\n");
							}

							if (user_field.getPrecision() == 0) {
								user_field.setPrecision(255);
							}
						} else if (user_field.getType() == FieldTypeEnum.DATE) {
							if (user_field.getCompiledDateFormat() == null) {
								message.append(
										"field {" + user_field.getName() + "}  in schema has invalid date format {"
												+ user_field.getParseFormat() + "}\n");
							}

							if (user_field.getDefaultValue() != null) {
								try {
									user_field.getDefaultDate();
								} catch (Throwable t) {
									message.append("field {" + user_field.getName()
											+ "}  in schema has invalid defaultValue\n");

								}
							}
						} else {
							message.append("field {" + user_field.getName() + "}  has invalid type  {"
									+ user_field.getType() + "}\n");
						}

						if (user_field.isMultiValue()) {
							if (user_field.getMultiValueSeparator() == null) {
								message.append("field {" + user_field.getName()
										+ "}  in schema must have 'multiValueSeparator' value when 'isMultiValue' is 'true'\n");
							}

							if (user_field.isUniqueId) {
								message.append("MultiValue field {" + user_field.getName()
										+ "}  in schema cannot be used as UniqueID\n");
							}

							if (user_field.getType() != FieldTypeEnum.TEXT) {
								message.append("MultiValue field {" + user_field.getName()
										+ "}  in schema can only be of Type Text\n");
							}

						}

						if (user_field.isUniqueId) {
							if (user_field.getType() != FieldTypeEnum.TEXT) {
								message.append("Non Text field {" + user_field.getName()
										+ "}  in schema cannot be used as UniqueID\n");
							} else {
								uniqueIdfieldNames.add(user_field.getName());
							}
						}
					} else {
						message.append("fields[" + fieldCount + "]] in schema cannot be null\n");
					}

				} // End for

				if (uniqueIdfieldNames.size() > 1) {
					message.append("More than one field has 'isUniqueId' attribute set to true {" + uniqueIdfieldNames
							+ "}\n");
				}

			} else {
				if (user_fields == null || user_fields.isEmpty()) {
					message.append("fields] in schema cannot be null or empty\n");
				} else if (user_fields.size() > 1000) {
					message.append("fields] in schema cannot contain more than 1000 fields\n");
				}
			}
		}

		if (message.length() != 0) {
			throw new IllegalArgumentException(message.toString());
		}
	}

	public static FileSchema merge(FileSchema userSchema, FileSchema autoSchema, PrintStream logger) {
		FileSchema mergedSchema = null;
		try {
			if (userSchema == null) {
				return autoSchema;
			}
			List<FieldType> user_fields = userSchema.getFields();
			List<FieldType> auto_fields = autoSchema.getFields();
			LinkedList<FieldType> merged_fields = new LinkedList<FieldType>();
			if (user_fields == null || user_fields.isEmpty()) {
				user_fields = auto_fields;
			}
			if (auto_fields == null || auto_fields.isEmpty()) {
				auto_fields = user_fields;
			}
			for (FieldType auto_field : auto_fields) {
				FieldType merged_field = null;
				boolean found = false;
				for (FieldType user_field : user_fields) {
					if (auto_field.getName().equals(user_field.getName())) {
						found = true;
						if (!auto_field.equals(user_field)) {
							logger.println("Field {" + user_field + "} has been modified by user");
							merged_field = new FieldType(user_field);
							merged_field.setName(
									user_field.getName() != null ? user_field.getName() : auto_field.getName());
							merged_field.setType(
									user_field.getType() != null ? user_field.getType() : auto_field.getType());
							merged_field
									.setDefaultValue(user_field.getDefaultValue() != null ? user_field.getDefaultValue()
											: auto_field.getDefaultValue());
							merged_field
									.setDescription(user_field.getDescription() != null ? user_field.getDescription()
											: auto_field.getDescription());
							merged_field
									.setParseFormat(user_field.getParseFormat() != null ? user_field.getParseFormat()
											: auto_field.getParseFormat());
							merged_field.isMultiValue = user_field.isMultiValue != false ? user_field.isMultiValue
									: auto_field.isMultiValue;
							merged_field.isUniqueId = user_field.isUniqueId != false ? user_field.isUniqueId
									: auto_field.isUniqueId;
							merged_field.setLabel(
									user_field.getLabel() != null ? user_field.getLabel() : auto_field.getLabel());
							merged_field.setMultiValueSeparator(
									user_field.getMultiValueSeparator() != null ? user_field.getMultiValueSeparator()
											: auto_field.getMultiValueSeparator());
							merged_field.setPrecision(user_field.getPrecision() != 0 ? user_field.getPrecision()
									: auto_field.getPrecision());
							merged_field.setScale(
									user_field.getScale() != 0 ? user_field.getScale() : auto_field.getScale());
						}
					}
				}
				if (!found) {
					logger.println("Found new field {" + auto_field + "} in CSV");
				}
				if (merged_field == null) {
					merged_field = auto_field;
				}
				merged_fields.add(merged_field);
			}

			mergedSchema = new FileSchema();
			mergedSchema.parseOptions = userSchema.parseOptions != null ? userSchema.parseOptions
					: autoSchema.parseOptions;
			mergedSchema.fields = merged_fields;
		} catch (Throwable t) {
			t.printStackTrace(logger);
		}
		return mergedSchema;
	}

	public static void mergeExtendedFields(FileSchema extSchema, FileSchema baseSchema, PrintStream logger) {
		try {
			if (extSchema == null) {
				return;
			}
			List<FieldType> ext_fields = extSchema.getFields();
			List<FieldType> base_fields = baseSchema.getFields();
			if (ext_fields == null || ext_fields.isEmpty()) {
				return;
			}
			if (base_fields == null || base_fields.isEmpty()) {
				return;
			}
			for (FieldType base_field : base_fields) {
				boolean found = false;
				for (FieldType ext_field : ext_fields) {
					if (base_field.getName().equals(ext_field.getName())) {
						found = true;
					}
				}
				if (!found) {
					logger.println("Found new field {" + base_field + "} in CSV");
				}
			}

		} catch (Throwable t) {
			t.printStackTrace(logger);
		}
		return;
	}

	public static File getSchemaFile(File csvFile, PrintStream logger) {
		try {
			if (!csvFile.getName().toUpperCase().endsWith(SCHEMA_FILE_SUFFIX)) {
				csvFile = new File(csvFile.getParent(),
						 FileUtilsExt.getBaseName(csvFile) + SCHEMA_FILE_SUFFIX);
				return csvFile;
			} else {
				return csvFile;
			}
		} catch (Throwable t) {
			t.printStackTrace(logger);
		}
		return null;
	}

	public static String[] createUniqueDevName(List<String> headers) {
		if (headers == null)
			return null;
		LinkedList<String> originalColumnNames = new LinkedList<String>();
		LinkedList<String> uniqueColumnNames = new LinkedList<String>();
		LinkedList<String> devNames = new LinkedList<String>();
		for (int i = 0; i < headers.size(); i++) {
			originalColumnNames.add(headers.get(i));
			String devName = createDevName(headers.get(i), "Column", i, true);
			devNames.add(devName);
		}

		for (int i = 0; i < headers.size(); i++) {
			String newName = devNames.get(i);
			if (uniqueColumnNames.contains(newName)) {
				int index = 1;
				while (true) {
					int maxLength = 40 - (index + "").length();
					if (devNames.get(i).length() > maxLength) {
						newName = devNames.get(i).substring(0, maxLength) + index;
					} else {
						newName = devNames.get(i) + index;
					}
					if (!uniqueColumnNames.contains(newName)
							&& !originalColumnNames.subList(i + 1, devNames.size()).contains(newName)) {
						break;
					}
					index++;
				}
			} else {
				// Did we change the column name? if yes check if have a collision with existing
				// columns
				if ((headers.get(i) == null || !newName.equals(headers.get(i)))
						&& originalColumnNames.subList(i + 1, devNames.size()).contains(newName)) {
					int index = 1;
					while (true) {
						int maxLength = 40 - (index + "").length();
						if (devNames.get(i).length() > maxLength) {
							newName = devNames.get(i).substring(0, maxLength) + index;
						} else {
							newName = devNames.get(i) + index;
						}
						if (!uniqueColumnNames.contains(newName)
								&& !originalColumnNames.subList(i + 1, devNames.size()).contains(newName)) {
							break;
						}
						index++;
					}
				}
			}
			uniqueColumnNames.add(newName);
		}
		return uniqueColumnNames.toArray(new String[0]);
	}

	public static String createDevName(String inString, String defaultName, int columnIndex,
			boolean allowCustomFieldExtension) {
		String outString = inString;
		String suffix = null;
		int maxLength = 120;
		if (defaultName != null) {
			if (defaultName.equalsIgnoreCase("object") || defaultName.equalsIgnoreCase("column")) {
				maxLength = 120;
			}
		}
		try {
			if (inString != null && !inString.trim().isEmpty()) {
				StringBuffer outStr = new StringBuffer(inString.length() + 1);
				if (allowCustomFieldExtension && inString.endsWith("__c") && !inString.equals("__c")) {
					suffix = "__c";
					inString = inString.substring(0, inString.length() - 3);
					maxLength = maxLength - suffix.length();
				}
				@SuppressWarnings("unused")
				int index = 0;
				boolean hasFirstChar = false;
				boolean lastCharIsUnderscore = false;
				for (char ch : inString.toCharArray()) {
					if (isLatinLetter(ch) || isLatinNumber(ch)) {
						if (!hasFirstChar && isLatinNumber(ch)) {
							outStr.append('X');
						}
						outStr.append(ch);
						hasFirstChar = true;
						lastCharIsUnderscore = false;
					} else if (hasFirstChar && !lastCharIsUnderscore) {
						outStr.append('_');
						lastCharIsUnderscore = true;
					}
					index++;
				}
				if (!hasFirstChar) {
					outString = defaultName + (columnIndex + 1);
				} else {
					outString = outStr.toString();
					if (outString.length() > maxLength) {
						outString = outString.substring(0, maxLength);
					}
					while (outString.endsWith("_") && outString.length() > 0) {
						outString = outString.substring(0, outString.length() - 1);
					}
					if (outString.isEmpty()) {
						outString = defaultName + (columnIndex + 1);
					} else {
						if (suffix != null)
							outString = outString + suffix;
					}
				}
			} else {
				outString = defaultName + (columnIndex + 1);
			}
		} catch (Throwable t) {
			t.printStackTrace();
		}
		return outString;
	}

	@SuppressWarnings("unused")
	private static String replaceString(String original, String pattern, String replace) {
		if (original != null && !original.isEmpty() && pattern != null && !pattern.isEmpty() && replace != null) {
			final int len = pattern.length();
			int found = original.indexOf(pattern);

			if (found > -1) {
				StringBuffer sb = new StringBuffer();
				int start = 0;

				while (found != -1) {
					sb.append(original.substring(start, found));
					sb.append(replace);
					start = found + len;
					found = original.indexOf(pattern, start);
				}

				sb.append(original.substring(start));

				return sb.toString();
			} else {
				return original;
			}
		} else
			return original;
	}

	public static boolean isLatinLetter(char c) {
		return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z');
	}

	public static boolean isLatinNumber(char c) {
		return (c >= '0' && c <= '9');
	}

	public static boolean isLatinNumber(String str) {
		if (str == null) {
			return false;
		}
		int sz = str.length();
		if (sz == 0) {
			return false;
		}

		for (int i = 0; i < sz; i++) {
			char c = str.charAt(i);
			if (!(c >= '0' && c <= '9'))
				return false;
		}
		return true;
	}

	@Override
	public String toString() {
		try {
			ObjectMapper mapper = new ObjectMapper();
			mapper.setSerializationInclusion(Include.NON_NULL);
			return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(this);
		} catch (Throwable t) {
			t.printStackTrace();
			return super.toString();
		}
	}

	public byte[] toBytes() {
		try {
			ObjectMapper mapper = new ObjectMapper();
			mapper.setSerializationInclusion(Include.NON_NULL);
			return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(this).getBytes(Charset.forName("UTF-8"));
		} catch (Throwable t) {
			t.printStackTrace();
			return super.toString().getBytes();
		}

	}

	public static FileSchema load(InputStream inputStream, Charset fileCharset, PrintStream logger)
			throws JsonParseException, JsonMappingException, IOException {
		ObjectMapper mapper = new ObjectMapper();
		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		InputStreamReader reader = new InputStreamReader(new BOMInputStream(inputStream, false),
				StringUtilsExt.utf8Decoder(null, Charset.forName("UTF-8")));
		FileSchema userSchema = mapper.readValue(reader, FileSchema.class);
		if (userSchema == null) {
			throw new IllegalArgumentException("Could not read schema from stream {null}");
		}
		validateSchema(userSchema, logger);
		return userSchema;
	}

	public static FileSchema load(String inputStream, PrintStream logger)
			throws JsonParseException, JsonMappingException, IOException {
		ObjectMapper mapper = new ObjectMapper();
		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		FileSchema userSchema = mapper.readValue(inputStream, FileSchema.class);
		if (userSchema == null) {
			throw new IllegalArgumentException("Could not read schema from stream {null}");
		}
		validateSchema(userSchema, logger);
		return userSchema;
	}

	public static boolean hasUniqueID(FileSchema inSchema) {
		boolean hasUniqueID = false;
		if (inSchema != null && inSchema.fields != null && inSchema.fields.size() > 0) {
			hasUniqueID = hasUniqueID(inSchema.getFields());
		}
		return hasUniqueID;
	}

	private static boolean hasUniqueID(List<FieldType> fields) {
		boolean hasUniqueID = false;
		if (fields != null) {
			for (FieldType user_field : fields) {
				if (user_field != null && user_field.isUniqueId && user_field.getType() == FieldTypeEnum.TEXT)
					hasUniqueID = true;
			}
		}
		return hasUniqueID;
	}

	public void clearNumericParseFormat() {
		if (fields != null) {
			for (FieldType user_field : fields) {
				if (user_field != null && user_field.getType() != FieldTypeEnum.DATE
						&& user_field.getParseFormat() != null)
					user_field.setParseFormat(null);
			}
		}
	}

	private static void setUniqueId(FileSchema inSchema, HashSet<String> uniqueIdfieldNames) {
		if (inSchema != null && inSchema.fields != null && inSchema.fields.size() > 0) {
			for (FieldType user_field : inSchema.fields) {
				if (uniqueIdfieldNames.contains(user_field.getName()) && user_field.getType() == FieldTypeEnum.TEXT)
					user_field.isUniqueId = true;
			}
		}
	}

	// Before calling this make sure you have validated field type, precision, scale
	// and praseFormat values
	private static void validateDefaultValue(FieldType user_field, StringBuffer message) {
		if (user_field.getDefaultValue() != null) {
			if (user_field.getType() == FieldTypeEnum.NUMERIC) {
				try {
					new BigDecimal(user_field.getDefaultValue());
				} catch (Throwable t) {
					message.append("field {" + user_field.getName() + "}  in schema has invalid defaultValue\n");
				}
			}
		} else if (user_field.getType() == FieldTypeEnum.BOOLEAN) {
			if (!Boolean.TRUE.toString().equalsIgnoreCase(user_field.getDefaultValue())
					&& !Boolean.FALSE.toString().equalsIgnoreCase(user_field.getDefaultValue())) {
				message.append("field {" + user_field.getName() + "}  in schema has invalid defaultValue\n");
			}
		} else if (user_field.getType() == FieldTypeEnum.DATE) {
			try {
				SimpleDateFormat compiledDateFormat = new SimpleDateFormat(user_field.getParseFormat());
				compiledDateFormat.setTimeZone(TimeZone.getTimeZone("GMT")); // All dates must be in GMT
				compiledDateFormat.setLenient(false);
				compiledDateFormat.parse(user_field.getDefaultValue());
			} catch (Throwable t) {
				message.append("field {" + user_field.getName() + "}  in schema has invalid defaultValue\n");

			}
		} else if (user_field.getType() == FieldTypeEnum.TEXT) {
			if (user_field.getDefaultValue().length() > user_field.getPrecision()) {
				message.append("field {" + user_field.getName() + "}  in schema has invalid defaultValue\n");
			}
		} else {
			message.append("field {" + user_field.getName() + "}  has invalid type  {" + user_field.getType() + "}\n");
		}
	}

}
