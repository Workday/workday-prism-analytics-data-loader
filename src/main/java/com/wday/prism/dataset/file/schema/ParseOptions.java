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

import java.util.LinkedHashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class ParseOptions {

	private String fieldsDelimitedBy = ",";
	private String recordsDelimitedBy = null;
	private Character fieldsEnclosedBy = '"';
	private Character fieldsEnclosingCharacterEscapedBy = '"';
	private Boolean ignoreLeadingWhitespaces = true;
	private Boolean ignoreTrailingWhitespaces = true;

	private int headerLinesToIgnore = 1; // BY DEFAULT THERE IS A HEADER LINE IN CSV
	private Map<String, String> charset = new LinkedHashMap<String, String>();
	private Map<String, String> type = new LinkedHashMap<String, String>();

	@JsonIgnore
	private String charsetName = "UTF-8";

	public String getCharsetName() {
		return charsetName;
	}

	public void setCharsetName(String charsetName) {
		this.charsetName = charsetName;
	}

	public String getFieldsDelimitedBy() {
		return fieldsDelimitedBy;
	}

	public void setFieldsDelimitedBy(String fieldsDelimitedBy) {
		if (fieldsDelimitedBy != null && !fieldsDelimitedBy.isEmpty())
			this.fieldsDelimitedBy = fieldsDelimitedBy;
	}

	public char getFieldsEnclosedBy() {
		return fieldsEnclosedBy;
	}

	public void setFieldsEnclosedBy(char fieldsEnclosedBy) {
		this.fieldsEnclosedBy = fieldsEnclosedBy;
	}

	public int getHeaderLinesToIgnore() {
		return headerLinesToIgnore;
	}

	public void setHeaderLinesToIgnore(int numberOfLinesToIgnore) {
		this.headerLinesToIgnore = numberOfLinesToIgnore;
	}

	public Map<String, String> getCharset() {
		charset.put("id", "Encoding=" + getCharsetName());
		return charset;
	}

	public void setCharset(Map<String, String> charset) {
		if (charset != null && charset.containsKey("id")) {
			String temp = charset.get("id");
			if (temp != null && !temp.trim().isEmpty()) {
				setCharsetName(temp.replace("Encoding=", ""));
			}
		}
	}

	public Map<String, String> getType() {
		if (type != null && !type.containsKey("id"))
			type.put("id", "Schema_File_Type=Delimited");
		return type;
	}

	public void setType(Map<String, String> type) {
		this.type = type;
	}

	public ParseOptions() {
		super();
	}

	public ParseOptions(ParseOptions old) {
		super();
		if (old != null) {
			setCharsetName(old.getCharsetName());
			setFieldsDelimitedBy(old.getFieldsDelimitedBy());
			setFieldsEnclosedBy(old.getFieldsEnclosedBy());
			setHeaderLinesToIgnore(old.getHeaderLinesToIgnore());
		}
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((getCharsetName() == null) ? 0 : getCharsetName().hashCode());
		result = prime * result + ((getFieldsDelimitedBy() == null) ? 0 : getFieldsDelimitedBy().hashCode());
		result = prime * result + getFieldsEnclosedBy();
		result = prime * result + getHeaderLinesToIgnore();
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
		ParseOptions other = (ParseOptions) obj;
		if (getCharsetName() == null) {
			if (other.getCharsetName() != null) {
				return false;
			}
		} else if (!getCharsetName().equals(other.getCharsetName())) {
			return false;
		}
		if (getFieldsDelimitedBy() == null) {
			if (other.getFieldsDelimitedBy() != null) {
				return false;
			}
		} else if (!getFieldsDelimitedBy().equals(other.getFieldsDelimitedBy())) {
			return false;
		}
		if (getFieldsEnclosedBy() != other.getFieldsEnclosedBy()) {
			return false;
		}
		if (getHeaderLinesToIgnore() != other.getHeaderLinesToIgnore()) {
			return false;
		}
		return true;
	}

	public Character getFieldsEnclosingCharacterEscapedBy() {
		return fieldsEnclosingCharacterEscapedBy;
	}

	public void setFieldsEnclosingCharacterEscapedBy(Character fieldsEnclosingCharacterEscapedBy) {
		this.fieldsEnclosingCharacterEscapedBy = fieldsEnclosingCharacterEscapedBy;
	}

	public Boolean isIgnoreLeadingWhitespaces() {
		return ignoreLeadingWhitespaces;
	}

	public void setIgnoreLeadingWhitespaces(Boolean ignoreLeadingWhitespaces) {
		this.ignoreLeadingWhitespaces = ignoreLeadingWhitespaces;
	}

	public Boolean isIgnoreTrailingWhitespaces() {
		return ignoreTrailingWhitespaces;
	}

	public void setIgnoreTrailingWhitespaces(Boolean ignoreTrailingWhitespaces) {
		this.ignoreTrailingWhitespaces = ignoreTrailingWhitespaces;
	}

	public String getRecordsDelimitedBy() {
		return recordsDelimitedBy;
	}

	public void setRecordsDelimitedBy(String recordsDelimitedBy) {
		this.recordsDelimitedBy = recordsDelimitedBy;
	}

}
