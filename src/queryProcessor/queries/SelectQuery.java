package queryProcessor.queries;

import database.Database;
import queryProcessor.commands.SelectCommand;
import queryProcessor.whereTree.WhereTree;

import java.util.ArrayList;

/**
 * Handles a Query in the form of
 * <p>
 * select * from <name>;
 * <name> is the name of the table. Table names are unique in the system.
 * <p>
 * This will display all of the data in the table in an easy to read format. This includes column
 * names.
 * An error will be reported if the table does not exist.
 */
public class SelectQuery extends Query {

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

    public SelectQuery(SelectCommand command, Database database) {
        super(database);
        isSelectStar = command.isSelectStar();
        selectTablesAndColumns = command.getSelectTablesAndColumns();
        fromTables = command.getFromTables();
        whereTree = command.getWhereTree();
        orderbyTableAndColumn = command.getOrderbyTableAndColumn();
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


    public boolean handleQuery() {
        return this.database.select(this);
    }
}
