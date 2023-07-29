package queryProcessor.commands;

import queryProcessor.whereTree.WhereTree;

import java.util.ArrayList;
import java.util.HashSet;

public class SelectCommand extends Command {

    boolean isSelectStar;

    // need to have 2D list to store tablenames for each column
    // sense multiple tables can have same column name ex: foo.x bar.x need to know which x we are referring to.
    // if the first index (the table name the column refers to) is null
    // that means the table name was not specified which means there is no ambiguity and that only
    // one column from all of the tables from the from clause has that column name, you will have to search for it.

    // if it is select * THIS WILL BE NULL
    private ArrayList<ArrayList<String>> selectTablesAndColumns; // [ [table, column], [foo, x], [bar, x] ]

    private ArrayList<String> fromTables; // ex: ["foo", "bar"]

    private WhereTree whereTree; // null if doesnt exist

    // orderby foo.x;    ["foo", "x"]            if assumed one x column: orderby x;    [null, "x"]
    private ArrayList<String> orderbyTableAndColumn; // null if doesnt exist

    // used to figure out where strings are and used to see if the right side of a clause is a string or column
    private HashSet<Integer> setOfIndicesInCurrentStringCommandListWithString;

    public SelectCommand(ArrayList<String> tokens, HashSet<Integer> setOfIndicesInCurrentStringCommandListWithString) {
        super(tokens);
        this.isSelectStar = false;
        this.selectTablesAndColumns = new ArrayList<>();
        this.fromTables = new ArrayList<>();
        this.whereTree = null;
        this.orderbyTableAndColumn = new ArrayList<>();
        this.setOfIndicesInCurrentStringCommandListWithString = setOfIndicesInCurrentStringCommandListWithString;
    }


    /**
     * Parses string from user input into form
     * <p>
     * select * from <name>;
     * <name> is the name of the table. Table names are unique in the system.
     */
    public Boolean parseCommand() {
        int tokensSize = tokens.size();
        tokens.remove(0); // select token
        extractSelectTablesAndColumns();
        tokens.remove(0); // from token
        extractFromTables();
        if(tokens.get(0).equals("where")){
            ArrayList<Object> whereList = extractWhereList(setOfIndicesInCurrentStringCommandListWithString, tokensSize); // otherwise we need to extract the where clause and put it into a tree
            whereTree = new WhereTree(whereList);
        }
        if(tokens.get(0).equals("orderby")){
            tokens.remove(0); // remove orderby token
            extractOrderbyTableAndColumn();
        } else {
            orderbyTableAndColumn = null; // no orderby clause
        }
        return true;
    }


    private void extractSelectTablesAndColumns(){
        while (!tokens.get(0).equals("from")){
            if(tokens.get(0).equals("*")){
                tokens.remove(0);
                isSelectStar = true;
                selectTablesAndColumns = null; // not used if select *
                break;
            } else {
                String tableAndColumn = tokens.remove(0);
                if(tableAndColumn.contains(".")){ // foo.x
                    String[] tableAndColumnTokens = tableAndColumn.split("\\.");
                    String table = tableAndColumnTokens[0];
                    String column = tableAndColumnTokens[1];
                    ArrayList<String> lst = new ArrayList<>();
                    lst.add(table);
                    lst.add(column);
                    selectTablesAndColumns.add(lst);
                } else { // table and column is really just a column like x
                    String column = tableAndColumn;
                    ArrayList<String> lst = new ArrayList<>();
                    lst.add(null);
                    lst.add(column);
                    selectTablesAndColumns.add(lst);
                }
                if(tokens.get(0).equals(",")){
                    tokens.remove(0);
                }
            }
        }
    }


    private void extractFromTables(){
        while (!tokens.get(0).equals(";") && !tokens.get(0).equals("where") && !tokens.get(0).equals("orderby")){
            fromTables.add(tokens.remove(0)); // remove the table name and add to list
            if(tokens.get(0).equals(",")){
                tokens.remove(0);
            }
        }
    }


    private void extractOrderbyTableAndColumn(){
        String tableAndColumn = tokens.remove(0);
        if(tableAndColumn.contains(".")){ // foo.x
            String[] tableAndColumnTokens = tableAndColumn.split("\\.");
            String table = tableAndColumnTokens[0];
            String column = tableAndColumnTokens[1];
            orderbyTableAndColumn.add(table);
            orderbyTableAndColumn.add(column);
        } else { // table and column is really just a column like x
            String column = tableAndColumn;
            ArrayList<String> lst = new ArrayList<>();
            lst.add(null);
            lst.add(column);
            orderbyTableAndColumn.add(null);
            orderbyTableAndColumn.add(column);
        }
    }


    public boolean isSelectStar() {
        return isSelectStar;
    }


    public ArrayList<ArrayList<String>> getSelectTablesAndColumns() {
        return selectTablesAndColumns;
    }


    public ArrayList<String> getFromTables() {
        return fromTables;
    }


    public WhereTree getWhereTree() {
        return whereTree;
    }


    public ArrayList<String> getOrderbyTableAndColumn() {
        return orderbyTableAndColumn;
    }

}
