package exception;

public class InvalidDataTypeWhenCreatingTableException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public InvalidDataTypeWhenCreatingTableException(String message) {
        super(message);
    }
}
