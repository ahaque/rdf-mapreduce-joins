package lubm.sortmerge;

import java.util.Arrays;

import org.apache.hadoop.hbase.KeyValue;

public class LUBMSharedServices {
	
	private static byte[] qualifier_gradStudent = "ub_GraduateStudent".getBytes();
	private static byte[] qualifier_undergradStudent = "ub_UndergraduateStudent".getBytes();
	
	public static boolean isStudent(KeyValue kv) {
		if (Arrays.equals(kv.getQualifier(), qualifier_gradStudent)) {
			return true;
		} else if (Arrays.equals(kv.getQualifier(), qualifier_undergradStudent)) {
			return true;
		}
		return false;
	}
	
}
