package queryProcessor.queries;

import database.Attribute;
import database.Database;
import queryProcessor.commands.CreateTableCommand;

import java.util.ArrayList;


/**
 * Handles a Query in the form of
 * <p>
 * create table <name>(
 * <attr_name1> <attr_type1> primarykey,
 * <attr_name2> <attr_type2>,
 * ....
 * <attr_nameN> <attr_typeN>
 * );
 *
 * <name> is the name of the table. Table names are unique in the system.
 * <attr name> is the name of the attribute. Attribute names are unique within a table.
 * <attr type> is the type of the attribute. These types are outlined above.
 * primarykey is the attribute that is the primary key of the table. The table can have
 * <p>
 * ex:
 * create table foo( num integer primarykey );
 * create table foo( age char(10), num integer primarykey );
 * <p>
 * This schema will be added to
 * the catalog. This schema will be used by the system to store/access/update/delete data in
 * the created table.
 * <p>
 * The command should report ”SUCCESS” if the table is successfully created. ”ERROR” and
 * a reason for the error if the table is not created
 */
public class CreateTableQuery extends Query {

    private final ArrayList<Attribute> attributes;

    private String name;


    public CreateTableQuery(CreateTableCommand command, Database database) {
        super(database);
        attributes = command.getAttributes();
        name = command.getName();
    }


    public ArrayList<Attribute> getAttributes() {
        return attributes;
    }


    public String getName() {
        return name;
    }


    public boolean handleQuery() {
        return this.database.createTable(this); // return true if success
    }
}
