package exception;


/**
 * @author Connor Hunter
 * <p>
 * A class for throwing NoPrimaryKeyDefinedException exceptions given a message
 * occurs when a user does not give a primary key when trying to create a table
 */
public class NoPrimaryKeyDefinedException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public NoPrimaryKeyDefinedException(String message) {
        super(message);
    }
}
