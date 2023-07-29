package queryProcessor.whereTree;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Stack;

public class WhereTree {

    private final ArrayList<Object> whereList;

    private Node root;

    /**
     * start with infix expression
     * convert the infix expression to postfix using shunting yard
     * construct the tree given a postfix expression
     * traverse the tree infix (as you traverse you evaluate expressions)
     */
    public WhereTree(ArrayList<Object> whereList) {
        this.root = null;
        this.whereList = convertInfixWhereListToPostOrderList(whereList);
        constructWhereTree();
    }


    private ArrayList<Object> convertInfixWhereListToPostOrderList(ArrayList<Object> whereList) {
        Stack<Object> stack = new Stack<>();
        Queue<Object> queue = new LinkedList<>();
        boolean isClause = true; // clause and/or clause and/or  need to know what you are currently parsing
        while (!whereList.isEmpty()) {
            if (isClause) {
                Object column = whereList.remove(0);
                Object relOp = whereList.remove(0);
                Object value = whereList.remove(0);
                queue.add(column);
                queue.add(relOp);
                queue.add(value);
                isClause = false;
            } else { // we are at an and/or token in the list
                // and has higher precedence than or
                String andOr = (String) whereList.remove(0);
                if (andOr.equals("and")) {
                    if (stack.isEmpty()) {
                        stack.push(andOr);
                    } else {
                        String topStackAndOr = (String) stack.peek();
                        if (topStackAndOr.equals("and")) { // top stack is and andOr is and
                            stack.push(andOr);
                        } else { // topStackAndOr.equals("or")  top stack is or and andOr is and
                            Object removedAndOr = stack.pop();
                            queue.add(removedAndOr);
                            stack.push(andOr);
                        }
                    }
                } else { // (andOr.equals("or")
                    if (stack.isEmpty()) {
                        stack.push(andOr);
                    } else {
                        String topStackAndOr = (String) stack.peek();
                        if (topStackAndOr.equals("and")) { // top stack is and andOr is or
                            Object removedAndOr = stack.pop();
                            queue.add(removedAndOr);
                            stack.push(andOr);
                        } else { // topStackAndOr.equals("or")  top stack is or and andOr is or
                            stack.push(andOr);
                        }
                    }
                }
                isClause = true;
            }
        }
        // now we need to populate the queue with whatever is left on the stack
        while (!stack.isEmpty()) {
            Object obj = stack.pop();
            queue.add(obj);
        }
        //next we need to populate the new whereList that is in postfix not infix
        while (!queue.isEmpty()) {
            whereList.add(queue.remove());
        }
        // where list is now in postfix
        return whereList;
    }


    // does POSTFIX tree construction
    // https://www.baeldung.com/cs/postfix-expressions-and-expression-trees
    private void constructWhereTree() {
        // [column, relop, value, and/or, column, relop, value, ...]

        Stack<Object> stack = new Stack<>();
        Node rightNode;
        Node leftNode;
        if(whereList.size() == 3){ // only one node edge case no ands/ors
            ArrayList<String> rightColumn = (ArrayList<String>) whereList.remove(0);
            String rightRelOp = (String) whereList.remove(0);
            Object rightValue = whereList.remove(0);
            if(rightValue instanceof String || rightValue instanceof Integer || rightValue instanceof Double || rightValue instanceof Boolean){
                root = new Node(false, false, rightColumn, rightRelOp, rightValue, false);
            }else{
                root = new Node(false, false, rightColumn, rightRelOp, rightValue, true);
            }
            return;
        }

        while (!whereList.isEmpty()) {
            try {
                String possibleAndOr = (String) whereList.get(0);
                if (possibleAndOr.equals("and") || possibleAndOr.equals("or")) {
                    whereList.remove(0);
                    if (stack.peek() instanceof Node) {
                        rightNode = (Node) stack.pop();
                    } else {
                        Object rightValue = stack.pop();
                        String rightRelOp = (String) stack.pop();
                        ArrayList<String> rightColumn = (ArrayList<String>) stack.pop();
                        if(rightValue instanceof String || rightValue instanceof Integer || rightValue instanceof Double || rightValue instanceof Boolean){
                            rightNode = new Node(false, false, rightColumn, rightRelOp, rightValue, false);
                        }else{
                            rightNode = new Node(false, false, rightColumn, rightRelOp, rightValue, true);
                        }
                    }
                    if (stack.peek() instanceof Node) {
                        leftNode = (Node) stack.pop();
                    } else {
                        Object leftValue = stack.pop();
                        String leftRelOp = (String) stack.pop();
                        ArrayList<String> leftColumn = (ArrayList<String>) stack.pop();
                        if(leftValue instanceof String || leftValue instanceof Integer || leftValue instanceof Double || leftValue instanceof Boolean){
                            leftNode = new Node(false, false, leftColumn, leftRelOp, leftValue, false);
                        }else{
                            leftNode = new Node(false, false, leftColumn, leftRelOp, leftValue, true);
                        }

                    }
                    Node newNode = new Node(possibleAndOr.equals("and"), possibleAndOr.equals("or"), null, null, null, false);
                    newNode.setLeft(leftNode);
                    newNode.setRight(rightNode);
                    stack.push(newNode);
                } else { // the string is a comparison i.e = > < != <=
                    stack.push(whereList.remove(0));
                }

            } catch (ClassCastException e) { // value at index in original where list is not an andOr operator
                // push the column or value onto the list
                stack.push(whereList.remove(0));
            }
        }
        root = (Node) stack.pop();
    }


    /**
     * the 2D List is structured like this
     * [ [table0, column0] .....   [tableN-1, columnN-1] ]
     * and is for making it so that you know which table and column each object in the row belongs to.
     *
     *
     * foo bar
     * foo.x foo.y bar.x
     * [ "a", 1, "foo"] is an example of a row
     */
    public boolean includeRow(ArrayList<Object> row, ArrayList<ArrayList<String>> tableAndColumns) {
        boolean isEdgeCaseOneNodeTree = false;
        if(root.getLeft() == null && root.getRight() == null){
            isEdgeCaseOneNodeTree = true;
        }

        evaluateTree(root, row, tableAndColumns, isEdgeCaseOneNodeTree);
        return root.isTrueNode(); // if not true it must be false
    }


    /**
     * This function is for evaluating the tree in INORDER tree traversal. The boolean result is stored in
     * currentTreeEvaluationForGivenExpression
     */
    private Node evaluateTree(Node rootNode, ArrayList<Object> row, ArrayList<ArrayList<String>> tableAndColumns, boolean isEdgeCaseOneNodeTree) {
        if (rootNode == null) {
            return null;
        } else if (isEdgeCaseOneNodeTree) {
            checkExpressionIsTrueForGivenRow(row, tableAndColumns, root);
            return root;
        }

        Node left = evaluateTree(rootNode.getLeft(), row, tableAndColumns, false);
        if (rootNode.isAndNode() || rootNode.isOrNode()) {
            if (!left.isAndNode() && !left.isOrNode()) {
                checkExpressionIsTrueForGivenRow(row, tableAndColumns, left);
            }
        }

        Node right = evaluateTree(rootNode.getRight(), row, tableAndColumns, false);
        if (rootNode.isAndNode() || rootNode.isOrNode()) {
            if (!right.isAndNode() && !right.isOrNode()) {
                checkExpressionIsTrueForGivenRow(row, tableAndColumns, right);
            }
        }

        if (rootNode.isAndNode()) {
            rootNode.setTrueNode(left.isTrueNode() && right.isTrueNode());
        } else if (rootNode.isOrNode()){
            rootNode.setTrueNode(left.isTrueNode() || right.isTrueNode());
        }
        return rootNode;
    }


    private void checkExpressionIsTrueForGivenRow(ArrayList<Object> row, ArrayList<ArrayList<String>>
            tableAndColumns, Node node) {
        /**
         * Possibilies:
         * L is col, table ; R is col w table
         * L is col, table ; R is col no table
         * L is col, no table ; R is col w table
         * L is col, no table ; R is col no table
         * L is col, table ; R is a value
         * L is col, no table ; R is a value
         */

        boolean foundColumn = false;
        boolean foundValue = false;

        switch (node.getRelOp()) {
            case "=":
                if (node.isValueIsColumn()) { // R is a col
                    ArrayList<Object> nodeValue = (ArrayList<Object>) node.getValue();
                    int i = 0;
                    for (ArrayList<String> tableAndColumn : tableAndColumns) {
                        if (tableAndColumn.equals(node.getColumn())) {
                            foundColumn = true;
                            break;
                        } else if (node.getColumn().get(0) == null && tableAndColumn.get(1).equals(node.getColumn().get(1))) {
                            foundColumn = true;
                            break;
                        }
                        i++;
                    }

                    int j = 0;
                    for (ArrayList<String> tableAndColumn : tableAndColumns) {
                        if (tableAndColumn.equals(nodeValue)) {
                            foundValue = true;
                            break;
                        } else if (node.getColumn().get(0) == null && tableAndColumn.get(1).equals(nodeValue.get(1))) {
                            foundColumn = true;
                            break;
                        }
                        j++;
                    }
                    if(foundColumn && foundValue){
                        if(row.get(i).equals(row.get(j))){
                            node.setTrueNode(true);
                        }else {
                            node.setTrueNode(false);
                        }
                        return ;
                    }
                    node.setTrueNode(false);
                    return ;
                } else { // R is a value
                    int i = 0;
                    for (ArrayList<String> tableAndColumn : tableAndColumns) {
                        if (tableAndColumn.equals(node.getColumn())) {
                            foundColumn = true;
                            break;
                        } else if (node.getColumn().get(0) == null && tableAndColumn.get(1).equals(node.getColumn().get(1))) {
                            foundColumn = true;
                            break;
                        }
                        i++;
                    }
                    if(foundColumn){
                        if(row.get(i).equals(node.getValue())){
                            node.setTrueNode(true);
                        }else {
                            node.setTrueNode(false);
                        }
                        return ;
                    }
                    node.setTrueNode(false);
                    return ;
                }
            case ">":
                if (node.isValueIsColumn()) { // R is a col
                    ArrayList<Object> nodeValue = (ArrayList<Object>) node.getValue();
                    int i = 0;
                    for (ArrayList<String> tableAndColumn : tableAndColumns) {
                        if (tableAndColumn.equals(node.getColumn())) {
                            foundColumn = true;
                            break;
                        } else if (node.getColumn().get(0) == null && tableAndColumn.get(1).equals(node.getColumn().get(1))) {
                            foundColumn = true;
                            break;
                        }
                        i++;
                    }

                    int j = 0;
                    for (ArrayList<String> tableAndColumn : tableAndColumns) {
                        if (tableAndColumn.equals(nodeValue)) {
                            foundValue = true;
                            break;
                        }else if (node.getColumn().get(0) == null && tableAndColumn.get(1).equals(nodeValue.get(1))) {
                            foundColumn = true;
                            break;
                        }
                        j++;
                    }
                    if(foundColumn && foundValue){


                        if(row.get(i) instanceof String){

                            if(((String) row.get(i)).compareTo((String) row.get(j)) > 0){ // row.get(i) > row.get(j)
                                node.setTrueNode(true);
                            }else {
                                node.setTrueNode(false);
                            }
                            return ;

                        } else if (row.get(i) instanceof Integer) {

                            if(((Integer) row.get(i)).compareTo((Integer) row.get(j)) > 0){ // row.get(i) > row.get(j)
                                node.setTrueNode(true);
                            }else {
                                node.setTrueNode(false);
                            }
                            return ;
                        }else if (row.get(i) instanceof Double) {

                            if(((Double) row.get(i)).compareTo((Double) row.get(j)) > 0){ // row.get(i) > row.get(j)
                                node.setTrueNode(true);
                            }else {
                                node.setTrueNode(false);
                            }
                            return ;
                        }
                    }
                    node.setTrueNode(false);
                    return ;
                } else { // R is a value
                    int i = 0;
                    for (ArrayList<String> tableAndColumn : tableAndColumns) {
                        if (tableAndColumn.equals(node.getColumn())) {
                            foundColumn = true;
                            break;
                        } else if (node.getColumn().get(0) == null && tableAndColumn.get(1).equals(node.getColumn().get(1))) {
                            foundColumn = true;
                            break;
                        }
                        i++;
                    }
                    if(foundColumn){
                        if(row.get(i) instanceof String){

                            if(((String) row.get(i)).compareTo((String) node.getValue()) > 0){ // row.get(i) > value
                                node.setTrueNode(true);
                            }else {
                                node.setTrueNode(false);
                            }
                            return ;
                        } else if (row.get(i) instanceof Integer) {

                            if(((Integer) row.get(i)).compareTo((Integer) node.getValue()) > 0){ // row.get(i) > value
                                node.setTrueNode(true);
                            }else {
                                node.setTrueNode(false);
                            }
                            return ;
                        }else if (row.get(i) instanceof Double) {

                            if(((Double) row.get(i)).compareTo((Double) node.getValue()) > 0){ // row.get(i) > value
                                node.setTrueNode(true);
                            }else {
                                node.setTrueNode(false);
                            }
                            return ;
                        }
                    }
                    node.setTrueNode(false);
                    return ;
                }
            case "<":
                if (node.isValueIsColumn()) { // R is a col
                    ArrayList<Object> nodeValue = (ArrayList<Object>) node.getValue();
                    int i = 0;
                    for (ArrayList<String> tableAndColumn : tableAndColumns) {
                        if (tableAndColumn.equals(node.getColumn())) {
                            foundColumn = true;
                            break;
                        }else if (node.getColumn().get(0) == null && tableAndColumn.get(1).equals(node.getColumn().get(1))) {
                            foundColumn = true;
                            break;
                        }
                        i++;
                    }

                    int j = 0;
                    for (ArrayList<String> tableAndColumn : tableAndColumns) {
                        if (tableAndColumn.equals(nodeValue)) {
                            foundValue = true;
                            break;
                        }else if (node.getColumn().get(0) == null && tableAndColumn.get(1).equals(nodeValue.get(1))) {
                            foundColumn = true;
                            break;
                        }
                        j++;
                    }
                    if(foundColumn && foundValue){
                        if(row.get(i) instanceof String){
                            if(((String) row.get(i)).compareTo((String) row.get(j)) < 0){ // row.get(i) < row.get(j)
                                node.setTrueNode(true);
                            }else {
                                node.setTrueNode(false);
                            }
                            return ;
                        } else if (row.get(i) instanceof Integer) {
                            if(((Integer) row.get(i)).compareTo((Integer) row.get(j)) < 0){ // row.get(i) < row.get(j)
                                node.setTrueNode(true);
                            }else {
                                node.setTrueNode(false);
                            }
                            return ;
                        }else if (row.get(i) instanceof Double) {
                            if(((Double) row.get(i)).compareTo((Double) row.get(j)) < 0){ // row.get(i) < row.get(j)
                                node.setTrueNode(true);
                            }else {
                                node.setTrueNode(false);
                            }
                            return ;
                        }
                    }
                    node.setTrueNode(false);
                    return ;
                } else { // R is a value
                    int i = 0;
                    for (ArrayList<String> tableAndColumn : tableAndColumns) {
                        if (tableAndColumn.equals(node.getColumn())) {
                            foundColumn = true;
                            break;
                        }else if (node.getColumn().get(0) == null && tableAndColumn.get(1).equals(node.getColumn().get(1))) {
                            foundColumn = true;
                            break;
                        }
                        i++;
                    }
                    if(foundColumn){
                        if(row.get(i) instanceof String){
                            if(((String) row.get(i)).compareTo((String) node.getValue()) < 0){ // row.get(i) < value
                                node.setTrueNode(true);
                            }else {
                                node.setTrueNode(false);
                            }
                            return ;
                        } else if (row.get(i) instanceof Integer) {
                            if(((Integer) row.get(i)).compareTo((Integer) node.getValue()) < 0){ // row.get(i) < value
                                node.setTrueNode(true);
                            }else {
                                node.setTrueNode(false);
                            }
                            return ;
                        }else if (row.get(i) instanceof Double) {
                            if(((Double) row.get(i)).compareTo((Double) node.getValue()) < 0){ // row.get(i) < value
                                node.setTrueNode(true);
                            }else {
                                node.setTrueNode(false);
                            }
                            return ;
                        }
                    }
                    node.setTrueNode(false);
                    return ;
                }
            case ">=":
                if (node.isValueIsColumn()) { // R is a col
                    ArrayList<Object> nodeValue = (ArrayList<Object>) node.getValue();
                    int i = 0;
                    for (ArrayList<String> tableAndColumn : tableAndColumns) {
                        if (tableAndColumn.equals(node.getColumn())) {
                            foundColumn = true;
                            break;
                        }else if (node.getColumn().get(0) == null && tableAndColumn.get(1).equals(node.getColumn().get(1))) {
                            foundColumn = true;
                            break;
                        }
                        i++;
                    }

                    int j = 0;
                    for (ArrayList<String> tableAndColumn : tableAndColumns) {
                        if (tableAndColumn.equals(nodeValue)) {
                            foundValue = true;
                            break;
                        }else if (node.getColumn().get(0) == null && tableAndColumn.get(1).equals(nodeValue.get(1))) {
                            foundColumn = true;
                            break;
                        }
                        j++;
                    }
                    if(foundColumn && foundValue){
                        if(row.get(i) instanceof String){
                            if(((String) row.get(i)).compareTo((String) row.get(j)) >= 0){ // row.get(i) >= row.get(j)
                                node.setTrueNode(true);
                            }else {
                                node.setTrueNode(false);
                            }
                            return ;
                        } else if (row.get(i) instanceof Integer) {
                            if(((Integer) row.get(i)).compareTo((Integer) row.get(j)) >= 0){ // row.get(i) >= row.get(j)
                                node.setTrueNode(true);
                            }else {
                                node.setTrueNode(false);
                            }
                            return ;
                        }else if (row.get(i) instanceof Double) {
                            if(((Double) row.get(i)).compareTo((Double) row.get(j)) >= 0){ // row.get(i) >= row.get(j)
                                node.setTrueNode(true);
                            }else {
                                node.setTrueNode(false);
                            }
                            return ;
                        }
                    }
                    node.setTrueNode(false);
                    return ;
                } else { // R is a value
                    int i = 0;
                    for (ArrayList<String> tableAndColumn : tableAndColumns) {
                        if (tableAndColumn.equals(node.getColumn())) {
                            foundColumn = true;
                            break;
                        }else if (node.getColumn().get(0) == null && tableAndColumn.get(1).equals(node.getColumn().get(1))) {
                            foundColumn = true;
                            break;
                        }
                        i++;
                    }
                    if(foundColumn){
                        if(row.get(i) instanceof String){
                            if(((String) row.get(i)).compareTo((String) node.getValue()) >= 0){ // row.get(i) >= value
                                node.setTrueNode(true);
                            }else {
                                node.setTrueNode(false);
                            }
                            return ;
                        } else if (row.get(i) instanceof Integer) {
                            if(((Integer) row.get(i)).compareTo((Integer) node.getValue()) >= 0){ // row.get(i) >= value
                                node.setTrueNode(true);
                            }else {
                                node.setTrueNode(false);
                            }
                            return ;
                        }else if (row.get(i) instanceof Double) {
                            if(((Double) row.get(i)).compareTo((Double) node.getValue()) >= 0){ // row.get(i) >= value
                                node.setTrueNode(true);
                            }else {
                                node.setTrueNode(false);
                            }
                            return ;
                        }
                    }
                    node.setTrueNode(false);
                    return ;
                }
            case "<=":
                if (node.isValueIsColumn()) { // R is a col
                    ArrayList<Object> nodeValue = (ArrayList<Object>) node.getValue();
                    int i = 0;
                    for (ArrayList<String> tableAndColumn : tableAndColumns) {
                        if (tableAndColumn.equals(node.getColumn())) {
                            foundColumn = true;
                            break;
                        }else if (node.getColumn().get(0) == null && tableAndColumn.get(1).equals(node.getColumn().get(1))) {
                            foundColumn = true;
                            break;
                        }
                        i++;
                    }

                    int j = 0;
                    for (ArrayList<String> tableAndColumn : tableAndColumns) {
                        if (tableAndColumn.equals(nodeValue)) {
                            foundValue = true;
                            break;
                        }else if (node.getColumn().get(0) == null && tableAndColumn.get(1).equals(nodeValue.get(1))) {
                            foundColumn = true;
                            break;
                        }
                        j++;
                    }
                    if(foundColumn && foundValue){
                        if(row.get(i) instanceof String){
                            if(((String) row.get(i)).compareTo((String) row.get(j)) <= 0){ // row.get(i) <= row.get(j)
                                node.setTrueNode(true);
                            }else {
                                node.setTrueNode(false);
                            }
                            return ;
                        } else if (row.get(i) instanceof Integer) {
                            if(((Integer) row.get(i)).compareTo((Integer) row.get(j)) <= 0){ // row.get(i) <= row.get(j)
                                node.setTrueNode(true);
                            }else {
                                node.setTrueNode(false);
                            }
                            return ;
                        }else if (row.get(i) instanceof Double) {
                            if(((Double) row.get(i)).compareTo((Double) row.get(j)) <= 0){ // row.get(i) <= row.get(j)
                                node.setTrueNode(true);
                            }else {
                                node.setTrueNode(false);
                            }
                            return ;
                        }
                    }
                    node.setTrueNode(false);
                    return ;
                } else { // R is a value
                    int i = 0;
                    for (ArrayList<String> tableAndColumn : tableAndColumns) {
                        if (tableAndColumn.equals(node.getColumn())) {
                            foundColumn = true;
                            break;
                        }else if (node.getColumn().get(0) == null && tableAndColumn.get(1).equals(node.getColumn().get(1))) {
                            foundColumn = true;
                            break;
                        }
                        i++;
                    }
                    if(foundColumn){
                        if(row.get(i) instanceof String){
                            if(((String) row.get(i)).compareTo((String) node.getValue()) <= 0){ // row.get(i) <= value
                                node.setTrueNode(true);
                            }else {
                                node.setTrueNode(false);
                            }
                            return ;
                        } else if (row.get(i) instanceof Integer) {
                            if(((Integer) row.get(i)).compareTo((Integer) node.getValue()) <= 0){ // row.get(i) <= value
                                node.setTrueNode(true);
                            }else {
                                node.setTrueNode(false);
                            }
                            return ;
                        }else if (row.get(i) instanceof Double) {
                            if(((Double) row.get(i)).compareTo((Double) node.getValue()) <= 0){ // row.get(i) <= value
                                node.setTrueNode(true);
                            }else {
                                node.setTrueNode(false);
                            }
                            return ;
                        }
                    }
                    node.setTrueNode(false);
                    return ;
                }
            case "!=":
                if (node.isValueIsColumn()) { // R is a col
                    ArrayList<Object> nodeValue = (ArrayList<Object>) node.getValue();
                    int i = 0;
                    for (ArrayList<String> tableAndColumn : tableAndColumns) {
                        if (tableAndColumn.equals(node.getColumn())) {
                            foundColumn = true;
                            break;
                        }else if (node.getColumn().get(0) == null && tableAndColumn.get(1).equals(node.getColumn().get(1))) {
                            foundColumn = true;
                            break;
                        }
                        i++;
                    }

                    int j = 0;
                    for (ArrayList<String> tableAndColumn : tableAndColumns) {
                        if (tableAndColumn.equals(nodeValue)) {
                            foundValue = true;
                            break;
                        }else if (node.getColumn().get(0) == null && tableAndColumn.get(1).equals(nodeValue.get(1))) {
                            foundColumn = true;
                            break;
                        }
                        j++;
                    }
                    if(foundColumn && foundValue){
                        if(!row.get(i).equals(row.get(j))){ // row.get(i) != row.get(j)
                            node.setTrueNode(true);
                        }else {
                            node.setTrueNode(false);
                        }
                        return ;
                    }
                    node.setTrueNode(false);
                    return ;
                } else { // R is a value
                    int i = 0;
                    for (ArrayList<String> tableAndColumn : tableAndColumns) {
                        if (tableAndColumn.equals(node.getColumn())) {
                            foundColumn = true;
                            break;
                        }else if (node.getColumn().get(0) == null && tableAndColumn.get(1).equals(node.getColumn().get(1))) {
                            foundColumn = true;
                            break;
                        }
                        i++;
                    }
                    if(foundColumn){
                        if(!row.get(i).equals(node.getValue())){ // row.get(i) <= value
                            node.setTrueNode(true);
                        }else {
                            node.setTrueNode(false);
                        }
                        return ;
                    }
                    node.setTrueNode(false);
                    return ;
                }
        }
        node.setTrueNode(false);
    }


    public Node getRoot() {
        return root;
    }

}
