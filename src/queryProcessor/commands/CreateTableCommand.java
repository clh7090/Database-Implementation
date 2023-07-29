package queryProcessor.commands;

import database.Attribute;
import exception.*;
import queryProcessor.ConstraintType;
import queryProcessor.KeywordType;

import java.util.ArrayList;

public class CreateTableCommand extends Command {

    private final ArrayList<Attribute> attributes;

    private int primaryKeyCount;

    public CreateTableCommand(ArrayList<String> tokens) {
        super(tokens);
        attributes = new ArrayList<>();
        primaryKeyCount = 0;
    }


    /**
     * Parses string from user input into form
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
     */
    public Boolean parseCommand() {
        tokens.remove(0);
        tokens.remove(0);
        // tokens after ['create', 'table', 'name', '(' ...] has been removed.
        extractName();
        tokens.remove(0); // remove ( also
        // now guaranteed to start with ['attr_name1', 'attr_name2', 'primarykey'  ... ]
        while (!tokens.get(0).equals(")") && !tokens.get(0).equals(";")) {
            attributes.add(extractAttribute());
        }
        try {
            checkDuplicateAttributeNames();
            checkPrimaryKey();
            checkInvalidDataTypeWhenCreatingTableException();
            return true;
        } catch (NoTableAttributesDefinedException | NoPrimaryKeyDefinedException | ExtraPrimaryKeyDefinedException
                 | DuplicateAttributeNameException | InvalidDataTypeWhenCreatingTableException e) {
            System.err.println(e);
            return false;
        }
    }


    private void checkPrimaryKey() {
        for (Attribute attribute : attributes) {
            if (attribute.isPrimaryKey() == 1) {
                primaryKeyCount++;
            }
        }
        if (primaryKeyCount == 0) {
            throw new NoPrimaryKeyDefinedException("ERROR: NO PRIMARY KEY DEFINED");
        }
        if (primaryKeyCount > 1) {
            throw new ExtraPrimaryKeyDefinedException("ERROR: MORE THAN ONE PRIMARY KEY NOT ALLOWED");
        }
    }


    private void checkDuplicateAttributeNames() throws DuplicateAttributeNameException {
        ArrayList<String> names = new ArrayList();
        if(attributes.size() == 0){
            throw new NoTableAttributesDefinedException("ERROR: NO ATTRIBUTES GIVEN");
        }
        for (Attribute attribute : attributes) {
            if (names.contains(attribute.getName())) {
                throw new DuplicateAttributeNameException(String.format("ERROR: DUPLICATE ATTRIBUTE NAMES NOT ALLOWED: attribute name: %s ", attribute.getName()) );
            } else {
                names.add(attribute.getName());
            }
        }
    }


    public void checkInvalidDataTypeWhenCreatingTableException() {
        for (Attribute attribute : attributes) {
            if (!attribute.getType().get(0).equals("integer") &&
                    !attribute.getType().get(0).equals("double") &&
                    !attribute.getType().get(0).equals("boolean") &&
                    !attribute.getType().get(0).equals("char") &&
                    !attribute.getType().get(0).equals("varchar")) {
                throw new InvalidDataTypeWhenCreatingTableException(String.format("ERROR: YOU TRIED TO CREATE A TABLE WITH A BAD COLUMN TYPE : attribute type: %s", attribute.getType().get(0)));
            }

        }
    }


    private Attribute extractAttribute() {
        String name;
        ArrayList<String> type;
        int isPrimaryKey = 0;
        boolean isUnique = false;
        boolean isNotNull = false;

        name = tokens.get(0);
        tokens.remove(0);

        if (tokens.get(0).equals(KeywordType.CHAR.toString()) || tokens.get(0).equals(KeywordType.VARCHAR.toString())) {
            String arg1 = tokens.get(0);
            tokens.remove(0);
            tokens.remove(0); // remove (
            String arg2 = tokens.get(0);
            tokens.remove(0);
            tokens.remove(0); // remove )
            type = new ArrayList<String>();
            type.add(arg1);
            type.add(arg2);
        } else { // something like integer only 1st arg matters
            String arg1 = tokens.get(0);
            tokens.remove(0);
            type = new ArrayList<String>();
            type.add(arg1); // array list is 1 in length
            type.add(null);
        }

        while (!tokens.get(0).equals(",") && !tokens.get(0).equals(")") && !tokens.get(0).equals(";")) {
            if (tokens.get(0).equals(ConstraintType.PRIMARY_KEY.toString())) {
                isPrimaryKey = 1;
                tokens.remove(0);
            } else if(tokens.get(0).equals(ConstraintType.UNIQUE.toString())){
                isUnique = true;
                tokens.remove(0);
            } else if(tokens.get(0).equals(ConstraintType.NOTNULL.toString())){
                isNotNull = true;
                tokens.remove(0);
            }
        }
        if (tokens.get(0).equals(",")) {
            tokens.remove(0);
        }
        return new Attribute(name, type, isPrimaryKey, isNotNull, isUnique);
    }


    public String getName() {
        return name;
    }


    public ArrayList<Attribute> getAttributes() {
        return attributes;
    }

}