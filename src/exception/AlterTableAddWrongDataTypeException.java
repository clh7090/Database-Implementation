package exception;

public class AlterTableAddWrongDataTypeException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public AlterTableAddWrongDataTypeException(String message) {
        super(message);
    }
}
