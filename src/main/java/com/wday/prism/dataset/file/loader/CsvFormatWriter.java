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
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;

import com.wday.prism.dataset.constants.Constants;
import com.wday.prism.dataset.file.schema.FieldType;
import com.wday.prism.dataset.file.schema.FieldTypeEnum;
import com.wday.prism.dataset.util.CSVWriter;
import com.wday.prism.dataset.util.StringUtilsExt;

/**
 * @author pgupta
 */
public class CsvFormatWriter {

	LinkedList<Integer> measure_index = new LinkedList<Integer>();
	List<FieldType> _dataTypes = new LinkedList<FieldType>();
	LinkedHashMap<String, Object> prev = new LinkedHashMap<String, Object>();;

	private int numColumns = 0;
	private final PrintStream logger;
	int interval = 10;

	private final File outputDir;
	private final String uploadFilePrefix;
	private File ouputFile = null;
	private CSVWriter writer = null;

	private volatile AtomicInteger successRowCount = new AtomicInteger(0);
	private volatile AtomicInteger totalRowCount = new AtomicInteger(0);
	private volatile AtomicLong totalOutputFileSize = new AtomicLong(0L);

	public static final NumberFormat nf = NumberFormat.getIntegerInstance();
	long startTime = 0L;

	private int filePart;
	private int headerLinesToIgnore;
	boolean parseContent;

	public CsvFormatWriter(File outputDir, String uploadFilePrefix, List<FieldType> dataTypes, PrintStream logger,
			int headerLinesToIgnore, boolean parseContent) {
		this.outputDir = outputDir;
		this.uploadFilePrefix = uploadFilePrefix;
		this._dataTypes = dataTypes;
		this.numColumns = dataTypes.size();
		this.logger = logger;
		this.headerLinesToIgnore = headerLinesToIgnore;
		this.parseContent = parseContent;
	}

	public void addrow(List<String> values) throws ParseException, FileNotFoundException, IOException {
		LinkedList<String> dim_values = new LinkedList<String>();
		LinkedHashMap<String, Object> curr = new LinkedHashMap<String, Object>();

		totalRowCount.incrementAndGet();

		if (totalRowCount.get() % interval == 0 || totalRowCount.get() == 1) {
			long newStartTime = System.currentTimeMillis();
			if (startTime == 0)
				startTime = newStartTime;
			logger.println("Processing row {" + nf.format(totalRowCount) + "} time {"
					+ nf.format(newStartTime - startTime) + "}");
			startTime = newStartTime;
		}

		if (interval < 1000000 && totalRowCount.get() / interval >= 10) {
			interval = interval * 10;
		}

		if (!parseContent) {
			initWriter();
			writer.writeRecord(values);
			successRowCount.incrementAndGet();
			return;
		}

		if (values.size() != this.numColumns) {
			String message = "Row " + totalRowCount + " contains an invalid number of Values, expected "
					+ this.numColumns + " Value(s), got " + values.size() + ".";
			throw new ParseException(message, 0);
		}

		for (int key_value_count = 0; key_value_count < this.numColumns; key_value_count++) {

			FieldType fieldType = _dataTypes.get(key_value_count);

			Object columnValue = fieldType.getDefaultValue();
			if (fieldType.getType() == FieldTypeEnum.DATE)
				columnValue = fieldType.getDefaultDate();

			if (values.get(key_value_count) != null) {
				columnValue = values.get(key_value_count);
			}

			if (fieldType.getType() == FieldTypeEnum.NUMERIC) {
				try {
					if (columnValue == null || columnValue.toString().trim().isEmpty()
							|| columnValue.toString().trim().equalsIgnoreCase("null")
							|| columnValue.toString().trim().equalsIgnoreCase("n/a")) {
						dim_values.add(null);
					} else if (columnValue != null) {
						BigDecimal v = null;
						if (columnValue instanceof Double)
							v = new BigDecimal((Double) columnValue);
						else if (fieldType.getCompiledNumberFormat() != null) {
							DecimalFormat indf = fieldType.getCompiledNumberFormat();
							indf.setParseBigDecimal(true);
							v = (BigDecimal) indf.parse(columnValue.toString().trim());
						} else
							v = new BigDecimal(columnValue.toString().trim());
						v = v.setScale(fieldType.getScale(), RoundingMode.UNNECESSARY);

						if (fieldType.getScale() == 0)
							curr.put(fieldType.getName(), v.longValue());
						else
							curr.put(fieldType.getName(), v.doubleValue());

						dim_values.add(v.toPlainString());
					}
				} catch (Throwable t) {
					throw new ParseException(fieldType.getName() + " is not a valid Decimal, value {" + columnValue
							+ "} " + (t.getMessage() != null ? t.getMessage() : ""), 0);
				}
			} else if (fieldType.getType() == FieldTypeEnum.DATE) {
				try {
					SimpleDateFormat sdt = fieldType.getCompiledDateFormat();
					if (columnValue == null || columnValue.toString().trim().isEmpty()
							|| columnValue.toString().trim().equalsIgnoreCase("null")
							|| columnValue.toString().trim().equalsIgnoreCase("n/a")) {
						dim_values.add(null);
						curr.put(fieldType.getName(), null);
					} else {
						Date dt = null;
						if (columnValue instanceof Date) {
							dt = (Date) columnValue;
						} else {
							dt = sdt.parse(columnValue.toString().trim());
						}
						dim_values.add(sdt.format(dt));
						curr.put(fieldType.getName(), sdt.format(dt));
					}
				} catch (Throwable t) {
					throw new ParseException(fieldType.getName() + " is not in specified date format {"
							+ fieldType.getParseFormat() + "}, value {" + columnValue + "} "
							+ (t.getMessage() != null ? t.getMessage() : ""), 0);
				}
			} else if (fieldType.getType() == FieldTypeEnum.BOOLEAN) {
				try {
					if (columnValue == null || columnValue.toString().trim().isEmpty()
							|| columnValue.toString().trim().equalsIgnoreCase("null")
							|| columnValue.toString().trim().equalsIgnoreCase("n/a")) {
						dim_values.add(null);
						curr.put(fieldType.getName(), null);
					} else {
						if (Boolean.TRUE.toString().equalsIgnoreCase(columnValue.toString().trim())
								|| Boolean.FALSE.toString().equalsIgnoreCase(columnValue.toString().trim())) {
							dim_values.add(columnValue.toString().trim().toLowerCase());
							curr.put(fieldType.getName(), columnValue.toString().trim().toLowerCase());
						} else {
							throw new ParseException(
									fieldType.getName() + " is not a valid boolean, value {" + columnValue + "}", 0);
						}
					}
				} catch (Throwable t) {
					throw new ParseException(
							fieldType.getName() + " is not a valid boolean, value {" + columnValue + "}", 0);
				}
			} else {
				if (columnValue != null && !columnValue.toString().trim().equalsIgnoreCase("null")) {
					dim_values.add(columnValue.toString());
				} else {
					dim_values.add(null);
				}
				curr.put(fieldType.getName(), columnValue);
			}
		}
		if (dim_values.size() == this.numColumns) {
			initWriter();
			writer.writeRecord(dim_values);
			successRowCount.incrementAndGet();
		} else {
			String message = "Row " + totalRowCount + " contains an invalid number of Values, expected "
					+ this.numColumns + " Value(s), got " + values.size() + ".";
			throw new ParseException(message, 0);
		}
		prev = curr;
	}

	public void finish() throws IOException {
		if (this.writer != null) {
			this.writer.close();
		}
		writer = null;
		if (this.ouputFile != null && this.ouputFile.exists())
			this.totalOutputFileSize.set(this.totalOutputFileSize.get() + this.ouputFile.length());
		this.ouputFile = null;
		if (totalRowCount.get() != 0) {
			long newStartTime = System.currentTimeMillis();
			if (startTime == 0)
				startTime = newStartTime;
			logger.println("Processed last row {" + nf.format(totalRowCount) + "} time {"
					+ nf.format(newStartTime - startTime) + "}");
			startTime = newStartTime;
		}
	}

	public String[] getHeader() {

		String[] hdr = new String[this._dataTypes.size()];
		for (int i = 0; i < this._dataTypes.size(); i++) {
			hdr[i] = this._dataTypes.get(i).getName();
		}
		return hdr;
	}

	public int getSuccessRowCount() {
		return successRowCount.get();
	}

	public int getTotalRowCount() {
		return totalRowCount.get();
	}

	public long getTotalOutputFileSize() {
		return this.totalOutputFileSize.get();
	}

	public int getFiscalWeek(int fiscalMonthOffset, int weeksinYear, int firstDayOfWeek, int year, int week) {
		Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
		calendar.setFirstDayOfWeek(firstDayOfWeek);
		calendar.set(Calendar.YEAR, year);
		calendar.set(Calendar.MONTH, fiscalMonthOffset);
		calendar.set(Calendar.DAY_OF_MONTH, 1);
		int weekOffset = calendar.get(Calendar.WEEK_OF_YEAR) - 1;
		int result = ((week - weekOffset - 1) % weeksinYear) + 1;
		if (result <= 0) {
			result += weeksinYear;
		}
		return result;
	}

	public void initWriter() throws FileNotFoundException, IOException {
		if (writer != null) {
			if (isMaxsizeReached()) {
				filePart++;
				this.writer.close();
				if (this.ouputFile != null && this.ouputFile.exists())
					this.totalOutputFileSize.set(this.totalOutputFileSize.get() + this.ouputFile.length());
				this.writer = null;
				this.ouputFile = null;
			}
		}

		if (writer == null) {
			if (filePart == 0) {
				this.ouputFile = new File(outputDir, uploadFilePrefix + ".csv.gz");
			} else {
				this.ouputFile = new File(outputDir, uploadFilePrefix + "." + filePart + ".csv.gz");
			}
			writer = new CSVWriter(new BufferedWriter(
					new OutputStreamWriter(new GzipCompressorOutputStream(new FileOutputStream(this.ouputFile)),
							StringUtilsExt.utf8Charset),
					Constants.DEFAULT_BUFFER_SIZE), ',', '"');
			writer.writeRecord(this.getHeader());
		}

	}

	public boolean isMaxsizeReached() {
		if (ouputFile != null && this.ouputFile.exists()
				&& this.ouputFile.length() > Constants.COMPRESSED_FILE_PART_LENGTH)
			return true;
		else
			return false;
	}

}
