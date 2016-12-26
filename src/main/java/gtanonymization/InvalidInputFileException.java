package gtanonymization;

/**
 * Specific Exception class for throwing Exception related to invalid input file format.
 * @author kanchan
 *
 */
public class InvalidInputFileException extends Exception {

 
	private static final long serialVersionUID = 123423423432L;

	public InvalidInputFileException() {
	}

	public InvalidInputFileException(String arg0) {
		super(arg0);
	}

	public InvalidInputFileException(Throwable arg0) {
		super(arg0);
	}

	public InvalidInputFileException(String arg0, Throwable arg1) {
		super(arg0, arg1);
	}

	public InvalidInputFileException(String arg0, Throwable arg1, boolean arg2, boolean arg3) {
		super(arg0, arg1, arg2, arg3);
	}

}
