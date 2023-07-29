package exception;

public class InvalidDataTypeInsertionException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public InvalidDataTypeInsertionException(String message) {
        super(message);
    }
}
