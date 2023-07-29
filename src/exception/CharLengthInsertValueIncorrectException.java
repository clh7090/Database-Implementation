package exception;

public class CharLengthInsertValueIncorrectException extends RuntimeException{
    private static final long serialVersionUID = 1L;

    public CharLengthInsertValueIncorrectException(String message) {
        super(message);
    }
}
