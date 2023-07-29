package exception;

public class InsertingNullIntoNotNullColumnException extends RuntimeException{
    private static final long serialVersionUID = 1L;

    public InsertingNullIntoNotNullColumnException(String message) {
        super(message);
    }
}
