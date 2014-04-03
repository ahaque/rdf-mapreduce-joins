import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class SharedServices {
	
	// Literal DATE and DATETIME formats
	private static DateTimeFormatter format1 = DateTimeFormat.forPattern("yyyy-MM-dd");
	private static DateTimeFormatter format2 = DateTimeFormat.forPattern("HH-mm-ss");
	
	// Possible Literal types
	private static enum Type {
		STRING, INT, DOUBLE, DATE, DATETIME
	}
	
	public static enum Tag {
		R1, R2, R3, R4, R5
	}
	
	public static byte[] CF_AS_BYTES = "o".getBytes();
	// Separates KEY from VALUE in intermediate MapReduce files
	public static char KEY_VALUE_DELIMITER = (char) 126;
	// Separates qualifier, timestamp, etc. in intermediate MapReduce files
	public static char SUBVALUE_DELIMITER = (char) 127;
	
	// Literal type mapping
	private static final HashMap<String, SharedServices.Type> literalTypeMap = new HashMap<String, SharedServices.Type>() {
		private static final long serialVersionUID = 5450689415960928404L;
	{
		put("dc_date",SharedServices.Type.DATE);
		put("dc_title", SharedServices.Type.STRING);
		put("rdfs_label", SharedServices.Type.STRING);
		put("rdfs_comment", SharedServices.Type.STRING);
		put("rev_reviewer", SharedServices.Type.STRING);
		put("rev_text", SharedServices.Type.STRING);
		put("bsbm-voc_productPropertyNumeric1", SharedServices.Type.INT);
		put("bsbm-voc_productPropertyNumeric2", SharedServices.Type.INT);
		put("bsbm-voc_productPropertyNumeric3", SharedServices.Type.INT);
		put("bsbm-voc_productPropertyNumeric5", SharedServices.Type.INT);
		put("bsbm-voc_productPropertyTextual1", SharedServices.Type.STRING);
		put("bsbm-voc_productPropertyTextual2", SharedServices.Type.STRING);
		put("bsbm-voc_productPropertyTextual3", SharedServices.Type.STRING);
		put("bsbm-voc_productPropertyTextual4", SharedServices.Type.STRING);
		put("bsbm-voc_productPropertyNumeric6", SharedServices.Type.INT);
		put("bsbm-voc_productPropertyTextual6", SharedServices.Type.STRING);
		put("bsbm-voc_productPropertyNumeric4", SharedServices.Type.INT);
		put("bsbm-voc_productPropertyTextual5", SharedServices.Type.STRING);
		put("bsbm-voc_price", SharedServices.Type.DOUBLE);
		put("bsbm-voc_validFrom", SharedServices.Type.DATETIME);
		put("bsbm-voc_validTo", SharedServices.Type.DATETIME);
		put("bsbm-voc_deliveryDays", SharedServices.Type.INT);
		put("bsbm-voc_reviewDate", SharedServices.Type.DATE);
	}};
	
	/*
	 * This method takes a KeyValue as an input and will return the String
	 * concatenation of the correct subject, predicate, and object. This method is
	 * necessary since the database stores most objects as columns except literals. In
	 * the case of literals, the column is the actual triple's predicate. Therefore
	 * we must output the correct String concatenation for both cases.
	 * 
	 * @param KeyValue kv - A single Hadoop KeyValue. Contains (key,value)=(subject,HBase cell)
	 * @return String[] result - String array of length 3 containing: <Subject>, <Predicate>, <Object>
	 */
	public static String[] keyValueToTripleString(KeyValue kv) throws IOException, ClassNotFoundException {
		String[] result = new String[4];
		/* If a literal then we need to:
    	 * 1. Use the column as the predicate
    	 * 2. Convert byte arrays to double/string/data format
    	 */	
    	String columnName = new String(kv.getBuffer(), kv.getQualifierOffset(), kv.getQualifierLength());
    	// integer literals
    	if (literalTypeMap.get(columnName) == SharedServices.Type.INT) {
    		byte[] rawBytes = kv.getValue();
			int number = ByteBuffer.wrap(rawBytes).getInt();
			
			result[0] = new String(kv.getBuffer(), kv.getKeyOffset(), kv.getKeyLength());
			result[1] = columnName;
			result[2] = number + "";
			return result;
    	}
    	// Type: double
    	else if (literalTypeMap.get(columnName) == SharedServices.Type.DOUBLE) {
    		byte[] rawBytes = kv.getValue();
			double number = ByteBuffer.wrap(rawBytes).getDouble();
			
			result[0] = new String(kv.getBuffer(), kv.getKeyOffset(), kv.getKeyLength());
			result[1] = columnName;
			result[2] = number + "";
			return result;
    	}
    	// String literals
    	else if (literalTypeMap.get(columnName) == SharedServices.Type.STRING) {
    		result[0] = new String(kv.getBuffer(), kv.getKeyOffset(), kv.getKeyLength());
			result[1] = columnName;
			result[2] = new String(kv.getValue());
			return result;
    	}
    	// Date literal
    	else if (literalTypeMap.get(columnName) == SharedServices.Type.DATE) {
    		byte[] rawBytes = kv.getValue();
    		long longDate = ByteBuffer.wrap(rawBytes).getLong();
    		// Use SQL date since we don't need HH:mm:ss
    		java.sql.Date date = new java.sql.Date(longDate);
    		result[0] = new String(kv.getBuffer(), kv.getKeyOffset(), kv.getKeyLength());
			result[1] = columnName;
			result[2] = date.toString();
			return result;
    	}
    	// DateTime
    	else if (literalTypeMap.get(columnName) == SharedServices.Type.DATETIME) { 
    		byte[] rawBytes = kv.getValue();
    		long longDate = ByteBuffer.wrap(rawBytes).getLong();
    		// Use java date since we need full date time
    		org.joda.time.DateTime d = new org.joda.time.DateTime(longDate);
    		result[0] = new String(kv.getBuffer(), kv.getKeyOffset(), kv.getKeyLength());
			result[1] = columnName;
			result[2] = format1.print(d) + "T" + format2.print(d);
			return result;
    	}
    	
    	// Object is not a literal
		result[0] = new String(kv.getBuffer(), kv.getKeyOffset(), kv.getKeyLength());
		result[1] = new String(kv.getBuffer(), kv.getValueOffset(), kv.getValueLength());
		result[2] = columnName;
		return result;
	}
	
	public static KeyValue[] listToArray(List<KeyValue> input) {
		KeyValue[] serializable = new KeyValue[input.size()];
		for (int i = 0; i < input.size(); i++) {
			serializable[i] = input.get(i);
		}
		return serializable;
	}
	
	// Takes a KeyValue as input, appends a tag, and outputs the new KeyValue
	public static KeyValue addTagToKv(KeyValue kv, KeyValue.Type type) {
		return new KeyValue(
				kv.getRow(),
				kv.getFamily(),
				kv.getQualifier(),
				kv.getTimestamp(),
				type,
				kv.getValue());
	}
	
	// Takes a KeyValue and creates a single-line String in HBase key:family:qualifier:timestamp:value format
	public static String keyValueToString(KeyValue kv) {
		StringBuilder builder = new StringBuilder();
		builder.append(new String(kv.getRow()));
    	builder.append(SharedServices.KEY_VALUE_DELIMITER);
    	builder.append(new String(kv.getRow()));
    	builder.append(SharedServices.SUBVALUE_DELIMITER);
    	builder.append(new String(kv.getFamily()));
    	builder.append(SharedServices.SUBVALUE_DELIMITER);
    	builder.append(new String(kv.getQualifier()));
    	builder.append(SharedServices.SUBVALUE_DELIMITER);
    	builder.append(kv.getTimestamp());
    	builder.append(SharedServices.SUBVALUE_DELIMITER);
    	builder.append(new String(kv.getValue()));
    	return builder.toString();
	}
	
	public static KeyValue stringToKeyValue(String line) {
		StringBuilder build = new StringBuilder();
		String[] tuple = new String[5];
		int currentIndex = 0;
		for (int i = 0; i < line.length(); i++) {
			char c = line.charAt(i);
			if (c == SharedServices.SUBVALUE_DELIMITER) {
				tuple[currentIndex] = build.toString();
				currentIndex++;
				build.setLength(0);
			} else {
				build.append(c);
			}
		}
		return new KeyValue(
				tuple[0].getBytes(),
				tuple[1].getBytes(),
				tuple[2].getBytes(),
				Long.parseLong(tuple[3]),
				tuple[4].getBytes()
				);
	}
}
