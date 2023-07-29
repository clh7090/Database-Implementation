package exception;

public class VarcharLengthExceededException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public VarcharLengthExceededException(String message) {
        super(message);
    }
}
