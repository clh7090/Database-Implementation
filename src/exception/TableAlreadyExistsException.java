package exception;

public class TableAlreadyExistsException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public TableAlreadyExistsException(String message) {
        super(message);
    }
}
