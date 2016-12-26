package gtanonymization.domain;

import java.util.Arrays;

public class DataMetadata {

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "DataMetadata [size=" + size + ", columns=" + Arrays.toString(columns) + "]";
	}

	public int size = 0;
	public ColumnMetadata[] columns = null;
	

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
