package org.am.cdo.util;
import java.util.ArrayList;
import java.util.List;

import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

import it.unimi.dsi.fastutil.floats.FloatArrayList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import tech.tablesaw.api.FloatColumn;
import tech.tablesaw.api.Table;
import tech.tablesaw.columns.Column;

public class AnalyticsUtil {

	public static List<Table> splitForBatch(int batchSize, Table dataTab) {
		batchSize = batchSize > 0 ? batchSize : 500;
		int parts = (int) Math.ceil((float)dataTab.rowCount() / (float)batchSize);
		List<Table> partitions = new ArrayList<Table>();
		
		int start = 0;
		int end = 0;
		for(int i=0; i < parts; i++) {
			end = (start + batchSize - 1) > dataTab.rowCount() ? dataTab.rowCount()-1 : start + batchSize - 1;
			partitions.add(dataTab.selectRows(start, end));
			start = end + 1;
		}
		System.out.println(String.format("Number of batches: %s , with batchSize: %s ", partitions.size(), batchSize));
		return partitions;
	}
	
	public static void readResultSetToTable(Table table, ResultSet rs) {
		List<Definition> colDefs = rs.getColumnDefinitions().asList();

		int columsSize = colDefs.size();
		for(Row row : rs) {
			for (int i = 0; i < columsSize; i++) {
                Column column = table.column(i);
                switch(colDefs.get(i).getType().toString()) {
	                case "date":
	                	column.appendCell(String.valueOf(row.getDate(i)));
	                	break;
	                case "double":
	                	column.appendCell(String.valueOf(row.getDouble(i)));
	                	break;
	                default:
	                	column.appendCell(row.getString(i));;
                }
            }
		}
	}
	
	public static void combinedFactCalcDaily(Table table1, String factors[]) {
		FloatColumn combined = null;
		Float weightage = 1.0f/factors.length;
		
		for (String factor : factors) {
			FloatColumn weightedfactor = table1.floatColumn(factor).multiply(weightage);
			
			if(combined == null) {
				combined = weightedfactor;
			} else {
				combined = combined.add(weightedfactor);
			}
		}
		
		//step 1 calc daily
		table1.addColumn(new FloatColumn("combined_calc_daily", combined.data()));
	}
	
	public static void combinedReturnDaily(Table table1, String factors[]) {
		// step 2 daily return
		IntArrayList secIds = table1.categoryColumn("security_id").data();
		FloatArrayList dailyCalc = table1.floatColumn("combined_calc_daily").data();
		FloatArrayList dailyReturns = new FloatArrayList();

		Integer prevId = null;
		for (int i = 0; i < secIds.size(); i++) {
			if (secIds.get(i).equals(prevId)) {
				dailyReturns.add((dailyCalc.get(i) - dailyCalc.get(i - 1)) / dailyCalc.get(i - 1));
			} else {
				dailyReturns.add(0);
			}
			prevId = secIds.get(i);
		}
		table1.addColumn(new FloatColumn("daily_return", dailyReturns));

	}

}
