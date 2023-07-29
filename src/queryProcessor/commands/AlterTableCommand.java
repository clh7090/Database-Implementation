package queryProcessor.commands;


import database.Attribute;
import queryProcessor.KeywordType;

import java.util.ArrayList;

public class AlterTableCommand extends Command{

    private String method; // add or drop

    private Attribute attribute;

    private Object defaultValue; // null unless they give one

    public AlterTableCommand(ArrayList<String> tokens) {
        super(tokens);
        this.method = null;
        this.attribute = null;
        this.defaultValue = null; // always starts as null
    }


    /**
     *
     alter table <name> drop <a_name>;
     alter table <name> add <a_name> <a_type>;
     alter table <name> add <a_name> <a_type> default <value>;
     */
    @Override
    public Boolean parseCommand() {
        tokens.remove(0);
        tokens.remove(0); // get rid of alter table
        extractName();
        extractRest();

        return true;
    }


    private void extractRest(){
        method = tokens.remove(0);
        String attributeName = tokens.remove(0);
        if(method.equals("drop")){ // drop does not need an attribute, only table name
            tokens.remove(0); // remove semi colon
            extractAttributeDrop(attributeName);
        }else if (method.equals("add"))
        { // add
            extractAttribute(attributeName);
        } else {
            // todo: error if method isn't "drop" or "add"
        }
    }


    private void extractAttributeDrop(String attributeName) {
        attribute = new Attribute(attributeName, null, 0, false, false);
    }



    private void extractAttribute(String attributeName) {
        ArrayList<String> type;

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

        if (tokens.get(0).equals(";")) {
            tokens.remove(0);
        }else{ // tokens.get(0).equals("default")
            tokens.remove(0);// remove default key word

            // parse defaultValue
            String defaultValueStr = tokens.remove(0);
            defaultValue = parseValue(defaultValueStr);

            tokens.remove(0); // remove ;
        }
        attribute = new Attribute(attributeName, type, 0, false, false);
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
}
