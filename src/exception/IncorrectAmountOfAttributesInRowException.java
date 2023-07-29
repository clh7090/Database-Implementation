package exception;

public class IncorrectAmountOfAttributesInRowException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public IncorrectAmountOfAttributesInRowException(String message) {
        super(message);
    }
}
