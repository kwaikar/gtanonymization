package gtanonymization.domain;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

import gtanonymization.Constants;

public class ColumnStatistics<T extends Comparable> {


	String columnName;
	char type;

	T min;
	T max;
	int range;
	/**
	 * @return the type
	 */
	public char getType() {
		return type;
	}

	/**
	 * @return the columnName
	 */
	public String getColumnName() {
		return columnName;
	}

	public ColumnStatistics(String columnName, char type) {
		super();
		this.columnName = columnName;
		this.type = type;
	}

	public ColumnStatistics(String columnName, Map<T,ValueMetadata<T>> map,char type) {
		super();
		this.columnName = columnName;
		this.map=map;
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
