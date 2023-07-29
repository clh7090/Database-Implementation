package exception;

public class InvalidBPlusTreeNode extends RuntimeException{
    private static final long serialVersionUID = 1L;

    public InvalidBPlusTreeNode(String message) {
        super(message);
    }
}
