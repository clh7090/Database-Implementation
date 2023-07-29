package queryProcessor.queries;

import database.Database;
import queryProcessor.commands.DropCommand;

public class DropQuery extends Query{

    private String name;
    public DropQuery(DropCommand command, Database database) {
        super(database);
        name = command.getName();
    }

    public String getName() {
        return name;
    }

    @Override
    public boolean handleQuery() {
        return this.database.dropTable(this);
    }
}
