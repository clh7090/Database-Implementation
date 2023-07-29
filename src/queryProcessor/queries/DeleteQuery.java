package queryProcessor.queries;

import database.Database;
import queryProcessor.whereTree.WhereTree;
import queryProcessor.commands.DeleteCommand;

public class DeleteQuery extends Query {

    private String name;

    private WhereTree whereTree;

    public DeleteQuery(DeleteCommand command, Database database) {
        super(database);
        name = command.getName();
        whereTree = command.getWhereTree();
    }


    public String getName() {
        return name;
    }


    public WhereTree getWhereTree() {
        return whereTree;
    }


    @Override
    public boolean handleQuery() {
        return this.database.delete(this);
    }
}
