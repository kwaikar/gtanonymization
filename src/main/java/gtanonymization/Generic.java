package gtanonymization;

/**
 * Base Class for representing top level value.
 * 
 * @author kanchan
 * @param <T>
 */
public class Generic<T> {

	T value;

	/**
	 * Total utility of the record
	 */
	double utilityOfRecord=0.0;
	

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((value == null) ? 0 : value.hashCode());
		return result;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (
			this == obj
		)
			return true;
		if (
			obj == null
		)
			return false;
		if (
			getClass() != obj.getClass()
		)
			return false;
		Generic other = (Generic) obj;
		if (
			value == null
		) {
			if (
				other.value != null
			)
				return false;
		}
		else if (
			!value.equals(other.value)
		)
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "Some [value=" + value + "]";
	}

}
