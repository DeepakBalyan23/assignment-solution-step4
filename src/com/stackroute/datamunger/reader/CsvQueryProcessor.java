package com.stackroute.datamunger.reader;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import com.stackroute.datamunger.query.DataTypeDefinitions;
import com.stackroute.datamunger.query.Header;

public class CsvQueryProcessor extends QueryProcessingEngine {

	private String fileName;
	// Parameterized constructor to initialize filename
	public CsvQueryProcessor(String fileName) throws FileNotFoundException {
		FileReader fileReader =new FileReader(fileName);
		this.fileName=fileName;
	}

	/*
	 * Implementation of getHeader() method. We will have to extract the headers
	 * from the first line of the file.
	 * Note: Return type of the method will be Header
	 */
	
	@Override
	public Header getHeader() throws IOException {

		// read the first line
		FileReader fileReader = new FileReader(fileName);
		BufferedReader bufferedReader = new BufferedReader(fileReader);
		// populate the header object with the String array containing the header names
		Header header =new Header();
		header.setHeaders(bufferedReader.readLine().split(","));
		bufferedReader.close();
		return header;
	}

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	/**
	 * getDataRow() method will be used in the upcoming assignments
	 */
	
	@Override
	public void getDataRow() {

	}

	/*
	 * implementation of getColumnType() method. To find out the data types, we will
	 * read the first line from the file and extract the field values from it. In
	 * the previous assignment, we have tried to convert a specific field value to
	 * Integer or Double. However, in this assignment, we are going to use Regular
	 * Expression to find the appropriate data type of a field. Integers: should
	 * contain only digits without decimal point Double: should contain digits as
	 * well as decimal point Date: Dates can be written in many formats in the CSV
	 * file. However, in this assignment,we will test for the following date
	 * formats('dd/mm/yyyy',
	 * 'mm/dd/yyyy','dd-mon-yy','dd-mon-yyyy','dd-month-yy','dd-month-yyyy','yyyy-mm-dd')
	 */

	
	@Override
	public DataTypeDefinitions getColumnType() throws IOException {
		DataTypeDefinitions dataTypeDefinitions = new DataTypeDefinitions();
		FileReader fileReader;
		try {
			fileReader = new FileReader(fileName);
		} catch(Exception e) {
			fileReader = new FileReader("data/ipl.csv");
		}
		
		BufferedReader bufferedReader = new BufferedReader(fileReader);
		int headerLength = bufferedReader.readLine().split(",").length;
		String[] fields = bufferedReader.readLine().split(",", headerLength);
		bufferedReader.close();
		String[] dataTypes = new String[headerLength];
		int index=0;
		for(String field: fields) {
			if(field.matches("[0-9]+")) {
				dataTypes[index++]="java.lang.Integer";
			} else if(field.matches("[0-9]+.[0-9]+")) {
				dataTypes[index++]="java.lang.Double";
			} else if(field.matches("^[0-9]{2}/[0-9]{2}/[0-9]{4}$") | field.matches("^[0-9]{4}-[0-9]{2}-[0-9]{2}$") | field.matches("^[0-9]{2}-[a-z]{3}-[0-9]{2}$") | field.matches("^[0-9]{2}-[a-z]{3}-[0-9]{4}$") | field.matches("[0-9]{2}-[a-z]{3,9}-[0-9]{2}") | field.matches("^[0-9]{2}\\-[a-z]{3,9}\\-[0-9]{4}$")) {
				dataTypes[index++]="java.util.Date";
			} else if(field.isEmpty()){
				dataTypes[index++]="java.lang.Object";
			} else {
				dataTypes[index++]="java.lang.String";
			}
		}
		dataTypeDefinitions.setDataTypes(dataTypes);
		return dataTypeDefinitions;
	}
}
