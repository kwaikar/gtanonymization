package gtanonymization.domain;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import gtanonymization.Constants;

/**
 * Holds metadata about the column.
 * 
 * @author kanchan
 * @param <T>
 */
public class ColumnMetadata<T extends Comparable> extends ColumnStatistics<T>{

	/**
	 * @return the range
	 */
	public int getRange() {
		return range;
	}

	/**
	 * @return the min
	 */
	public T getMin() {
		return min;
	}

	/**
	 * @return the max
	 */
	public T getMax() {
		return max;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "ColumnMetadata [columnName=" + columnName + ", type=" + type + ", min=" + min + ", max=" + max
				 + ", uniqueValues= " + map.keySet().size() + "]";
	}

	

	public ColumnMetadata(String columnName, char type) {
		super(columnName,type);
	}

	Map<T, Integer> indexMap = new HashMap<T, Integer>();

	Map<Integer,T> reverseIndexMap = new HashMap<Integer,T>();
	public int getNumUniqueValues() {
		return map.keySet().size();
	}

	public int getIndex(String entryString) {
		if (indexMap == null || indexMap.isEmpty()) {
			Set<T> keySet = map.keySet();
			int index = 0;
			for (T t : keySet) {
				reverseIndexMap.put(index, t);
				indexMap.put(t, index++);
			}
		}
		return indexMap.get(extractEntry(entryString));

	}

	public T getEntryAtPosition(int position) {
		 
		return reverseIndexMap.get(position);

	}
	public double getEntryFromMap(String entry) {
		return map.get(extractEntry(entry)).getProbability();
	}

	public T addEntryToMap(String entryString) {

		T entry = extractEntry(entryString);

		return super.addEntryToMap(entry);
	}

	
	private T extractEntry(String entryString) {
		Object entry = null;
		entryString = entryString.trim().replaceAll("\\$", "");
		switch (
			this.type
		) {
		/**
		 * Add integer value
		 */
		case 'i':
		case 'P':
			entry = (Integer.parseInt(entryString));
			break;

		/**
		 * Add double value
		 */
		case 'd':
		case '$':
			entry = (Double.parseDouble(entryString));
			break;
		/**
		 * add integer currency
		 */


		case 's':
			entry = (entryString.trim());
			break;
		}
		return (T) entry;
	}

}
