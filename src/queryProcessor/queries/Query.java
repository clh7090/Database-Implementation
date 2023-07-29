package queryProcessor.queries;

import database.Database;

/**
 * @author Connor Hunter
 * <p>
 * A parent class for children Queries.
 * This is used to store a given SQL Query made from a user command
 */
public abstract class Query {

    protected final Database database;

    public Query(Database database) {
        this.database = database;
    }

    public abstract boolean handleQuery();
}
