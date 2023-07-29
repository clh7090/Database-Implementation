package exception;

/**
 * @author Connor Hunter
 * <p>
 * A class for throwing ExtraPrimaryKeyDefinedException exceptions given a message
 * occurs when a user defines more thzn 1 primary key when trying to create a table.
 */
public class ExtraPrimaryKeyDefinedException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public ExtraPrimaryKeyDefinedException(String message) {
        super(message);
    }
}
