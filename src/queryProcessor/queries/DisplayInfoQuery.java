package queryProcessor.queries;

import database.Database;
import queryProcessor.commands.DisplayInfoCommand;


/**
 * Handles a Query in the form of
 * <p>
 * display info <name>;.
 *
 * <name> is the name of the table.
 * <p>
 * This command will display the information about a table in an easy to read format. Tt will
 * display:
 * <p>
 * table name
 * table schema
 * number of pages
 * number of records
 */
public class DisplayInfoQuery extends Query {

    private String name;

    public DisplayInfoQuery(DisplayInfoCommand command, Database database) {
        super(database);
        name = command.getName();
    }


    public String getName() {
        return name;
    }


    public boolean handleQuery() {
        boolean printIndented = false;
        return this.database.displayInfo(name, printIndented); // return true if success
    }
}
