package gtanonymization.domain;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

import gtanonymization.Constants;

public class ColumnStatistics<T extends Comparable> implements Serializable {

	/**
	 * @param min the min to set
	 */
	public void setMin(T min) {
		this.min = min;
	}

	/**
	 * @param max the max to set
	 */
	public void setMax(T max) {
		this.max = max;
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

	String columnName;
	char type;
	boolean isQuasiIdentifier;
	T min;
	T max;
	int range;
	int numUniqueValues=0;

	/**
	 * @return the numUniqueValues
	 */
	public int getNumUniqueValues() {
		return numUniqueValues;
	}

	/**
	 * @param numUniqueValues the numUniqueValues to set
	 */
	public void setNumUniqueValues(int numUniqueValues) {
		this.numUniqueValues = numUniqueValues;
	}

	/**
	 * @return the type
	 */
	public char getType() {
		return type;
	}

	/**
	 * @return the isQuasiIdentifier
	 */
	public boolean isQuasiIdentifier() {
		return isQuasiIdentifier;
	}

	/**
	 * @return the columnName
	 */
	public String getColumnName() {
		return columnName;
	}

	public ColumnStatistics(String columnName, char type, boolean isQuasiIdentifier) {
		super();
		this.columnName = columnName;
		this.type = type;
		this.isQuasiIdentifier=isQuasiIdentifier;
	}

	  
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "ColumnStatistics [columnName=" + columnName + ", type=" + type + ", isQuasiIdentifier="
				+ isQuasiIdentifier + ", min=" + min + ", max=" + max + ", range=" + range + ", numUniqueValues="
				+ numUniqueValues + ", map=" + map + "]";
	}

	public ColumnStatistics(String columnName, Map<T, ValueMetadata<T>> map, char type) {
		super();
		this.columnName = columnName;
		this.map = map;
		this.type = type;
	}

	Map<T, ValueMetadata<T>> map = new TreeMap<T, ValueMetadata<T>>();

	/**
	 * @return the map
	 */
	public Collection<ValueMetadata<T>> getValues() {
		return map.values();
	}

	public T addEntryToMap(T entry) {
		ValueMetadata<T> metadata = map.get(entry);
		if (metadata == null) {
			metadata = new ValueMetadata<T>(entry);
		}
		else {
			metadata.incrementFrequency();
		}
		map.put(entry, metadata);
		return entry;
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
	}
}
