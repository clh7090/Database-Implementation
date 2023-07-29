package exception;

public class AlterTableDropPrimaryKeyException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public AlterTableDropPrimaryKeyException(String message) {
        super(message);
    }
}