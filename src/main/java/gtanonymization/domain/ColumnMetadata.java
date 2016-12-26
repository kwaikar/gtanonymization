package gtanonymization.domain;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import gtanonymization.Constants;

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
				+ ", mode=" + mode + "]";
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

	public void addEntryToMap(T entry) {
		ValueMetadata<T> metadata = map.get(entry);
		if (metadata == null) {
			metadata = new ValueMetadata<T>(entry);
		}
		else {
			metadata.incrementCount();
		}
		map.put(entry, metadata);
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
			value.setPercentageValue((double) value.getCount() / totalCount);
		}
		this.mode = tempMode.getValue();
	}
}
