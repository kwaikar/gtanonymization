package gtanonymization.domain;

import java.io.Serializable;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Row object for holding tuples.
 * @author kanchan
 *
 */
public class Row  implements Serializable{

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "Row [id=" + id + "]";
	}
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public int id;
	/**
	 * @return the id
	 */
	public int getId() {
		return id;
	}

	/**
	 * @param id the id to set
	 */
	public void setId(int id) {
		this.id = id;
	}
	public Object [] row;

	public Pair<Object,Object>[] newRow;
 
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
	public Pair<Object,Object>[] getNewRow() {
		return newRow;
	}

	/**
	 * @param newRow the newRow to set
	 */
	public void setNewRow(Pair<Object,Object > newRow,int index) {
		this.newRow[index] = newRow;
	}
/**
 * Default constructor for registration with serializer
 */
	public Row() {
	}
	/**
	 * @param row
	 */
	public Row(Object[] row, ColumnStatistics[] columns) {
		super();
		this.row = row;
		this.newRow = new Pair[row.length];
		int i=0;
		while(i< newRow.length) {
			newRow[i]=new ImmutablePair(columns[i].getMin(), columns[i].getMax());
			i++;
			
		}
	}

}
