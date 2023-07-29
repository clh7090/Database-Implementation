package exception;

public class InsertingDuplicateValueIntoUniqueColumnException extends RuntimeException{
    private static final long serialVersionUID = 1L;

    public InsertingDuplicateValueIntoUniqueColumnException(String message) {
        super(message);
    }
}
