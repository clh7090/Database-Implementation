package queryProcessor.queries;

import database.Database;
import queryProcessor.whereTree.WhereTree;
import queryProcessor.commands.UpdateCommand;

public class UpdateQuery extends Query{

    private String name;

    private String updateColumn;

    private Object updateValue;

    private WhereTree whereTree;

    public UpdateQuery(UpdateCommand command, Database database) {
        super(database);
        this.name = command.getName();
        this.updateColumn = command.getUpdateColumn();
        this.updateValue = command.getUpdateValue();
        this.whereTree = command.getWhereTree();
    }

    @Override
    public boolean handleQuery() {
        return this.database.update(this);
    }

    public String getName() { return name; }


    public String getUpdateColumn() {
        return updateColumn;
    }


    public Object getUpdateValue() {
        return updateValue;
    }


    public WhereTree getWhereTree() {
        return whereTree;
    }

}
