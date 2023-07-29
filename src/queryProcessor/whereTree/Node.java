package queryProcessor.whereTree;

import java.util.ArrayList;

public class Node {

    private boolean isTrueNode;

    private final boolean isAndNode;

    private final boolean isOrNode;

    private final ArrayList<String> column; // column <= value  is one where clause     these values are null if it is and / or node

    private final String relOp; // > <= != =

    // BE CAREFUL where t1.a = t2.b and t2.c = t3.d    OR where bar = 10;   VALUE DOES NOT HAVE TO BE A INT/STRING/BOOL IT CAN BE ANOTHER COLUMN
    private final Object value; // IMPORTANT column can be a num a string 1, "test" OR IT CAN BE ANOTHER COLUMN CHECK PHASE 3 SELECT *

    private boolean valueIsColumn;

    private Node left;

    private Node right;


    public Node(boolean isAndNode, boolean isOrNode, ArrayList<String> column, String relOp, Object value, boolean valueIsColumn) {
        this.isTrueNode = false;
        this.isAndNode = isAndNode;
        this.isOrNode = isOrNode;
        this.column = column;
        this.relOp = relOp;
        this.value = value;
        this.left = null;
        this.right = null;
        this.valueIsColumn = valueIsColumn;
    }

    public boolean isTrueNode() {
        return isTrueNode;
    }

    public void setTrueNode(boolean bool) {
        isTrueNode = bool;
    }

    public boolean isAndNode() {
        return isAndNode;
    }


    public boolean isOrNode() {
        return isOrNode;
    }




    public ArrayList<String> getColumn() {
        return column;
    }


    public String getRelOp() {
        return relOp;
    }


    public Object getValue() {
        return value;
    }

    public Node getLeft() {
        return left;
    }

    public void setLeft(Node left) {
        this.left = left;
    }

    public Node getRight() {
        return right;
    }

    public void setRight(Node right) {
        this.right = right;
    }

    public boolean isValueIsColumn() {
        return valueIsColumn;
    }
}
