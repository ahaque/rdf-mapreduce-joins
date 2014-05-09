package lubm.sortmerge;

import java.util.Arrays;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import bsbm.sortmerge.SharedServices;


public class LUBMSharedServices {
	
	private static byte[] qualifier_gradStudent = "ub_GraduateStudent".getBytes();
	private static byte[] qualifier_undergradStudent = "ub_UndergraduateStudent".getBytes();
	
	/**
	 * @param KeyValue that contains the rdf_type
	 * @return If the KeyValue is a grad student
	 */
	public static boolean isStudent(KeyValue kv) {
		if (Arrays.equals(kv.getQualifier(), qualifier_gradStudent)) {
			return true;
		} else if (Arrays.equals(kv.getQualifier(), qualifier_undergradStudent)) {
			return true;
		}
		return false;
	}
	
	private static byte[] qualifier_assistantProf = "ub_AssistantProfessor".getBytes();
	private static byte[] qualifier_associateProf = "ub_AssociateProfessor".getBytes();
	private static byte[] qualifier_fullProf = "ub_FullProfessor".getBytes();
	private static byte[] qualifier_lecturer = "ub_Lecturer".getBytes();
	
	/**
	 * @param KeyValue that contains the rdf_type
	 * @return True/False if the key value is a faculty member
	 */
	public static boolean isFaculty(KeyValue kv) {
		if (Arrays.equals(kv.getQualifier(), qualifier_assistantProf)) {
			return true;
		} else if (Arrays.equals(kv.getQualifier(), qualifier_associateProf)) {
			return true;
		} else if (Arrays.equals(kv.getQualifier(), qualifier_fullProf)) {
			return true;
		} else if (Arrays.equals(kv.getQualifier(), qualifier_lecturer)) {
			return true;
		}
		return false;
	}
	
	
	private static byte[] qualifier_course = "ub_Course".getBytes();
	private static byte[] qualifier_graduateCourse = "ub_GraduateCourse".getBytes();
	
	/**
	 * @param KeyValue that contains the rdf_type
	 * @return True/False if the key value is a course
	 */
	public static boolean isCourse(KeyValue kv) {
		if (Arrays.equals(kv.getQualifier(), qualifier_course)) {
			return true;
		} else if (Arrays.equals(kv.getQualifier(), qualifier_graduateCourse)) {
			return true;
		}
		return false;
	}
	
	
	private static byte[] rdf_type_bytes = "rdf_type".getBytes();
	/**
	 * @param HBase Result row
	 * @return The rdf_type of the row
	 */
	public static String getTypeFromHBaseRow(Result row, String[] typesToCheck) {
		// For each type to check, get the column
		for (String type : typesToCheck) {
			byte[] objectValue = row.getValue(SharedServices.CF_AS_BYTES, type.getBytes());
			// If null, this is not the correct type so keep going
			if (objectValue == null) { continue; }
			// If it is a type, then we found the correct column (type)
			if (Arrays.equals(objectValue, rdf_type_bytes)) {
				return type;
			}
		}
		return null;
	}
	
	
	
	
	
	
}
