package gtanonymization.domain;

public class ValueMetadata<T extends Comparable> {

	/**
	 * @return the value
	 */
	public T getValue() {
		return value;
	}



	/**
	 * @param value the value to set
	 */
	public void setValue(T value) {
		this.value = value;
	}


	private T value;
	private int count;
	private double probability;


	public ValueMetadata(T value) {
		super();
		this.value = value;
		this.count = 1;
	}
	
	

	public void incrementCount() {
		this.count++;
	}

	/**
	 * @return the count
	 */
	public int getCount() {
		return count;
	}

	/**
	 * @param count
	 *            the count to set
	 */
	public void setCount(int count) {
		this.count = count;
	}

	/**
	 * @return the probability
	 */
	public double getProbability() {
		return probability;
	}

	/**
	 * @param probability
	 *            the probability to set
	 */
	public void setProbability(double probability) {
		this.probability = probability;
	}


	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "ValueMetadata [value=" + value + ", count=" + count + ", probability=" + probability + "]";
	}

}
