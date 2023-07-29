package queryProcessor.queries;

import database.Attribute;
import database.Database;
import queryProcessor.commands.AlterTableCommand;

public class AlterTableQuery extends Query{

    private String name;

    private String method; // add or drop

    private Attribute attribute;

    private Object defaultValue; // null unless they give one

    public AlterTableQuery(AlterTableCommand command, Database database) {
        super(database);
        name = command.getName();
        method = command.getMethod();
        attribute = command.getAttribute();
        defaultValue = command.getDefaultValue();
    }

    public String getName() {
        return name;
    }

    public String getMethod() {
        return method;
    }

    public Attribute getAttribute() {
        return attribute;
    }

    public Object getDefaultValue() {
        return defaultValue;
    }

    @Override
    public boolean handleQuery() {
        return this.database.alterTable(this);
    }
}
