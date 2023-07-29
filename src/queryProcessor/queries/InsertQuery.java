package queryProcessor.queries;

import database.Database;
import queryProcessor.commands.InsertCommand;

import java.util.ArrayList;

/**
 * Handles a Query in the form of
 * <p>
 * insert into <name> values <tuples>;
 * <p>
 * insert into: All DML statements that start with this will be considered to be trying
 * to insert data into a table. Both are considered keywords.
 * <name>: is the name of the table to insert into. All table names are unique.
 * values is considered a keyword.
 * <tuples>: A space separated list of tuples. A tuple is in the form: ( v1 ... vN )
 * <p>
 * Be aware just like in SQL, insert will insert a new tuple and not update an existing one. If it
 * tries to insert a tuple with the same primary key values as one that exists it will report an
 * error and stop adding tuples. Any tuples already added will remain. Any tuple remaining to
 * be added will not be added.
 * <p>
 * All primary key, data types, and not null constraints must be validated. Primary key values
 * can never be null.
 * <p>
 * Upon error the insertion process will stop. Any items inserted before the error will still be
 * inserted.
 * <p>
 * Note: null is a special value to represent there is no value for that attribute. Also note that
 * spaces are allowed in strings.
 */
public class InsertQuery extends Query {

    // 2d array list, can be multiple tuples to insert into table
    private final ArrayList<ArrayList<Object>> values;
    private String name;

    public InsertQuery(InsertCommand command, Database database) {
        super(database);
        values = command.getValues();
        name = command.getName();
    }


    public ArrayList<ArrayList<Object>> getValues() {
        return values;
    }


    public String getName() {
        return name;
    }


    public boolean handleQuery() {
        return database.insertIntoTable(this); // return true if success
    }
}
