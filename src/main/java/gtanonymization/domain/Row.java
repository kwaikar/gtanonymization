package gtanonymization.domain;

/**
 * Row object for holding tupples.
 * @author kanchan
 *
 */
public class Row {

	public Object[] row;

	public Object[] newRow;
 
	/**
	 * @return the row
	 */
	public Object getRow(int index) {
		return row[index];
	}

	/**
	 * @param row the row to set
	 */
	public void setRow(Object[] row) {
		this.row = row;
	}

	/**
	 * @return the newRow
	 */
	public Object[] getNewRow() {
		return newRow;
	}

	/**
	 * @param newRow the newRow to set
	 */
	public void setNewRow(Object newRow,int index) {
		this.newRow[index] = newRow;
	}

	/**
	 * @param row
	 */
	public Row(Object[] row) {
		super();
		this.row = row;
		this.newRow = new Object[row.length];
	}

}
