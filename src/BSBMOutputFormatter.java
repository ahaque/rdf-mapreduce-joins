import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.KeyValue;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;


public class BSBMOutputFormatter {
	
	private static DateTimeFormatter format1 = DateTimeFormat.forPattern("yyyy-MM-dd");
	private static DateTimeFormatter format2 = DateTimeFormat.forPattern("HH-mm-ss");
	
	/*
	 * This method takes a KeyValue as an input and will return the String
	 * concatenation of the correct subject, predicate, and object. This method is
	 * necessary since the database stores most objects as columns except literals. In
	 * the case of literals, the column is the actual triple's predicate. Therefore
	 * we must output the correct String concatenation for both cases.
	 * 
	 * @param KeyValue kv - A single Hadoop KeyValue. Contains (key,value)=(subject,HBase cell)
	 * @return String result - String in the form: <Subject> <Predicate> <Object>
	 */
	public static String[] keyValueToTripleString(KeyValue kv) throws IOException, ClassNotFoundException {
		String[] result = new String[3];
		/* If a literal then we need to:
    	 * 1. Use the column as the predicate
    	 * 2. Convert byte arrays to double/string/data format
    	 */
    	String columnName = new String(kv.getBuffer(), kv.getQualifierOffset(), kv.getQualifierLength());
    	// integer literals
    	if (columnName.contains("bsbm-voc_productPropertyNumeric") ||
    		columnName.equals("bsbm-voc_deliveryDays")) {
    		byte[] rawBytes = kv.getValue();
			int number = ByteBuffer.wrap(rawBytes).getInt();
			
			result[0] = new String(kv.getBuffer(), kv.getKeyOffset(), kv.getKeyLength());
			result[1] = new String(kv.getBuffer(), kv.getQualifierOffset(), kv.getQualifierLength());
			result[2] = number + "";
			return result;
    	}
    	// Type: double
    	else if (columnName.contains("bsbm-voc_price")) {
    		byte[] rawBytes = kv.getValue();
			double number = ByteBuffer.wrap(rawBytes).getDouble();
			
			result[0] = new String(kv.getBuffer(), kv.getKeyOffset(), kv.getKeyLength());
			result[1] = new String(kv.getBuffer(), kv.getQualifierOffset(), kv.getQualifierLength());
			result[2] = number + "";
			return result;
    	}
    	// String literals
    	else if (columnName.contains("bsbm-voc_productPropertyTextual")) {
    		result[0] = new String(kv.getBuffer(), kv.getKeyOffset(), kv.getKeyLength());
			result[1] = new String(kv.getBuffer(), kv.getQualifierOffset(), kv.getQualifierLength());
			result[2] = new String(kv.getValue());
			return result;
    	}
    	// Date literal
    	else if (columnName.equals("dc_date")) {
    		byte[] rawBytes = kv.getValue();
    		long longDate = ByteBuffer.wrap(rawBytes).getLong();
    		// Use SQL date since we don't need HH:mm:ss
    		java.sql.Date date = new java.sql.Date(longDate);
    		result[0] = new String(kv.getBuffer(), kv.getKeyOffset(), kv.getKeyLength());
			result[1] = new String(kv.getBuffer(), kv.getQualifierOffset(), kv.getQualifierLength());
			result[2] = date.toString();
			return result;
    	}
    	// DateTime
    	else if (columnName.equals("bsbm-voc_validTo") ||
    			 columnName.equals("bsbm-voc_validFrom")) { 
    		byte[] rawBytes = kv.getValue();
    		long longDate = ByteBuffer.wrap(rawBytes).getLong();
    		// Use java date since we need full datetime
    		org.joda.time.DateTime d = new org.joda.time.DateTime(longDate);
    		result[0] = new String(kv.getBuffer(), kv.getKeyOffset(), kv.getKeyLength());
			result[1] = new String(kv.getBuffer(), kv.getQualifierOffset(), kv.getQualifierLength());
			result[2] = format1.print(d) + "T" + format2.print(d);
			return result;
    	}
    	else if (columnName.equals("rdfs_label") ||
    			 columnName.equals("rdfs_comment")) { 
    		result[0] = new String(kv.getBuffer(), kv.getKeyOffset(), kv.getKeyLength());
    		result[1] = new String(kv.getBuffer(), kv.getQualifierOffset(), kv.getQualifierLength());
			result[2] = new String(kv.getBuffer(), kv.getValueOffset(), kv.getValueLength());
   			return result;
    	}
    	
    	// Object is not a literal
		result[0] = new String(kv.getBuffer(), kv.getKeyOffset(), kv.getKeyLength());
		result[1] = new String(kv.getBuffer(), kv.getValueOffset(), kv.getValueLength());
		result[2] = new String(kv.getBuffer(), kv.getQualifierOffset(), kv.getQualifierLength());
		return result;
	}
}
