package exception;

/**
 * @author Connor Hunter
 * <p>
 * A class for throwing DuplicatePrimaryKeyException exceptions given a message
 * occurs when a primary key value already exists in a record of a table and you try to add a duplicate.
 */
public class DuplicatePrimaryKeyException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public DuplicatePrimaryKeyException(String message) {
        super(message);
    }
}
