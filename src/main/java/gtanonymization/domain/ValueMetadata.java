package gtanonymization.domain;

/**
 * Metadata Class for holding value statistics.
 * @author kanchan
 *
 * @param <T>
 */
public class ValueMetadata<T extends Comparable> {

	private T value;
	private int frequency;
	private double probability;

	/**
	 * Constructor for ValueMetadata
	 * @param value
	 */
	public ValueMetadata(T value) {
		super();
		this.value = value;
		this.frequency = 1;
	}
	
	/**
	 * @return the value
	 */
	public T getValue() {
		return value;
	}

	/**
	 * @param value
	 *            the value to set
	 */
	public void setValue(T value) {
		this.value = value;
	}

	

	public void incrementFrequency() {
		this.frequency++;
	}

	/**
	 * @return the count
	 */
	public int getCount() {
		return frequency;
	}

	/**
	 * @param count
	 *            the count to set
	 */
	public void setCount(int count) {
		this.frequency = count;
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

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "ValueMetadata [value=" + value + ", frequency=" + frequency + ", probability=" + probability + "]";
	}

}
