package exception;

public class NoSuchTableException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public NoSuchTableException(String message) {
        super(message);
    }
}
