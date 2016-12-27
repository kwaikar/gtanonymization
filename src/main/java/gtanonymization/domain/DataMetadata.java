package gtanonymization.domain;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class DataMetadata {

	public int size = 0;
	public ColumnMetadata[] columns = null;
	public List<Object[]> rows = new LinkedList<Object[]>();
	public int numRangeColumns = 0;

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "DataMetadata [size=" + size + ", columns=" + Arrays.toString(columns) + ", numRangeColumns="
				+ numRangeColumns + "]";
	}

	public void addRow(Object[] row) {
		rows.add(row);
	}

	/**
	 * @return the numRangeColumns
	 */
	public int getNumRangeColumns() {
		return numRangeColumns;
	}

	/**
	 * @param numRangeColumns
	 *            the numRangeColumns to set
	 */
	public void setNumRangeColumns(int numRangeColumns) {
		this.numRangeColumns = numRangeColumns;
	}

	public DataMetadata(int size) {
		super();
		this.size = size;
		columns = new ColumnMetadata[size];
	}

	/**
	 * @return the columns
	 */
	public ColumnMetadata getColumn(int index) {
		return columns[index];
	}

	/**
	 * @param columns
	 *            the columns to set
	 */
	public void setColumn(int index, ColumnMetadata column) {
		this.columns[index] = column;
	}

}
