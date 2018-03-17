package org.am.cdo.util;
import java.io.*;
import java.util.*;

public class FileUtils {
	public static List<String> readFileIntoList(String filename) {

		List<String> fileList = new ArrayList<String>();
		BufferedReader br = null;
		File file = new File(filename);

		try {
			String currentLine;
			br = new BufferedReader(new FileReader(file));

			while ((currentLine = br.readLine()) != null) {
				fileList.add(currentLine);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (br != null)
					br.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}			
		}
		return fileList;
	}
	public static String readFileIntoString(String filename) {
	
		StringBuffer buffer = new StringBuffer();
		BufferedReader br = null;
		File file = new File(filename);

		try {
			String currentLine;
			br = new BufferedReader(new FileReader(file));

			while ((currentLine = br.readLine()) != null) {
				buffer.append(currentLine);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (br != null)
					br.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}			
		}
		return buffer.toString();
	}
}
