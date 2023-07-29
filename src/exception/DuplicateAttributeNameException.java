package exception;

/**
 * @author Connor Hunter
 * <p>
 * A class for throwing DuplicateAttributeNameException exceptions given a message
 * occurs when 2 names for an attribute are trying to be put in a table
 */
public class DuplicateAttributeNameException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public DuplicateAttributeNameException(String message) {
        super(message);
    }
}
