package queryProcessor.commands;


import java.util.ArrayList;

public class InsertCommand extends Command {

    // 2d array list, can be multiple tuples to insert into table
    private final ArrayList<ArrayList<Object>> values;

    public InsertCommand(ArrayList<String> tokens) {
        super(tokens);
        values = new ArrayList<>();
    }


    /**
     * Parses string from user input into form
     * <p>
     * insert into <name> values <tuples>;
     * <p>
     * insert into: All DML statements that start with this will be considered to be trying
     * to insert data into a table. Both are considered keywords.
     * <name>: is the name of the table to insert into. All table names are unique.
     * values is considered a keyword.
     * <tuples>: A space separated list of tuples. A tuple is in the form: ( v1 ... vN )
     */
    public Boolean parseCommand() {
        tokens.remove(0);
        tokens.remove(0);
        extractName();
        tokens.remove(0); // remove ( also
        // now guarenteed to start with ['attr_name1', 'attr_name2', 'primarykey'  ... ]
        while (!tokens.get(0).equals(";")) { // when we see closing paren were done with attributes.
            values.add(extractValue());
        }
        return true;
    }


    /**
     * Extracts a list of values and puts it into another list.
     */
    private ArrayList<Object> extractValue() {
        ArrayList<Object> valuesTuple = new ArrayList<>();
        if (tokens.get(0).equals("(")) {
            tokens.remove(0);
        }
        while (!tokens.get(0).equals(",") && !tokens.get(0).equals(")")) {
            Object value = parseValue(tokens.get(0));
            valuesTuple.add(value); // k
            tokens.remove(0);
        }
        if (tokens.get(0).equals(")")) {
            tokens.remove(0);
        }
        if (tokens.get(0).equals(",")) {
            tokens.remove(0);
        }
        if (tokens.get(0).equals("(")) {
            tokens.remove(0);
        }
        return valuesTuple;
    }


    public String getName() {
        return name;
    }


    public ArrayList<ArrayList<Object>> getValues() {
        return values;
    }

}
