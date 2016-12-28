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
public class ColumnMetadata<T extends Comparable> {

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
				+ ", mode=" + mode + ", uniqueValues= " + map.keySet().size() + "]";
	}

	/**
	 * @return the columnName
	 */
	public String getColumnName() {
		return columnName;
	}

	String columnName;
	char type;
	T min;
	T max;
	T mode;
	int range;

	public ColumnMetadata(String columnName, char type) {
		super();
		this.columnName = columnName;
		this.type = type;
	}

	/**
	 * @return the type
	 */
	public char getType() {
		return type;
	}

	/**
	 * @param type
	 *            the type to set
	 */
	public void setType(char type) {
		this.type = type;
	}

	Map<T, ValueMetadata<T>> map = new HashMap<T, ValueMetadata<T>>();

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

		ValueMetadata<T> metadata = map.get(entry);
		if (metadata == null) {
			metadata = new ValueMetadata<T>(entry);
		}
		else {
			metadata.incrementCount();
		}
		map.put(entry, metadata);
		return entry;
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

	public void setMinMaxAndMode() {
		Collection<T> keys = map.keySet();
		this.min = (T) Collections.min(keys);
		this.max = (T) Collections.max(keys);
		if ((min + "").matches(Constants.DOUBLE_REGEX)) {
			this.range = (int) (Double.parseDouble(max + "") - (Double.parseDouble(min + "")));
		}
		if ((min + "").matches(Constants.INT_REGEX)) {
			this.range = Integer.parseInt(max + "") - (Integer.parseInt(min + ""));
		}
		ValueMetadata<T> tempMode = null;
		int totalCount = 0;
		for (ValueMetadata<T> value : map.values()) {
			if (tempMode == null || tempMode.getCount() < value.getCount()) {
				tempMode = value;
			}
			totalCount += value.getCount();
		}
		for (ValueMetadata<T> value : map.values()) {
			value.setProbability((double) value.getCount() / totalCount);
		}
		this.mode = tempMode.getValue();
	}
}
