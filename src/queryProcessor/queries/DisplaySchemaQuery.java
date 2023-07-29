package queryProcessor.queries;

import database.Database;
import queryProcessor.commands.DisplaySchemaCommand;

/**
 * Handles a Query in the form of
 * <p>
 * display schema;
 * <p>
 * This command will display the catalog of the database in an easy to read format. For this
 * phase it will just display:
 * <p>
 * database location
 * page size
 * buffer size
 * table schema
 */
public class DisplaySchemaQuery extends Query {

    public DisplaySchemaQuery(DisplaySchemaCommand command, Database database) {
        super(database);
    }

    public boolean handleQuery() {
        return this.database.displaySchema(this); // return true if success
    }
}
