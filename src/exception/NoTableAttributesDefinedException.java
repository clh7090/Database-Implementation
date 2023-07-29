package exception;

/**
 * @author Connor Hunter
 * <p>
 * A class for throwing NoTableAttributesDefinedException exceptions given a message
 * occurs when a user does not define any attribute colums for a table when creating it.
 */
public class NoTableAttributesDefinedException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public NoTableAttributesDefinedException(String message) {
        super(message);
    }
}
