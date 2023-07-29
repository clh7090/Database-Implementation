package queryProcessor.commands;


import queryProcessor.KeywordType;

import java.util.*;

/**
 * @author Connor Hunter
 * <p>
 * A parent class for children Commands.
 * This is used to store a given command recieved from System.in
 */
public abstract class Command {

    protected String name;

    protected ArrayList<String> tokens;

    public Command(ArrayList<String> tokens) {
        this.tokens = tokens;
    }


    /**
     * name will always be the first due to how the tokens array was cleaned
     */
    protected void extractName() {
        name = tokens.remove(0);
    }


    public Object parseValue(String str) {
        Object value = null;

        switch (checkType(str)){
            case "null": value = null; break;
            case "string": value = str; break;
            case "integer": value = Integer.parseInt(str); break;
            case "double": value = Double.parseDouble(str); break;
            case "boolean": value = Boolean.parseBoolean(str); break;
        }

        return value;
    }

    /**
     * Check the type of a given string
     * @param value a string value
     * @return the string of the type i.e "'"integer"
     */
    public String checkType(String value){
        if(value.equals("null")){
            return "null";
        } else if (value.equals("true")) {
            return "boolean";
        } else if (value.equals("false")) {
            return "boolean";
        }

        // try to parse int
        try{
            Integer.parseInt(value);
            return "integer";
        } catch (NumberFormatException nfe) {
            try {
                Double.parseDouble(value);
                return "double";
            } catch (Exception e) {
                // not double must be string
                return "string"; // default
            }
        }
    }


    protected ArrayList<Object> extractWhereList(HashSet setOfIndicesInCurrentStringCommandListWithString, int allTokensSize){
        int curIndexInTokensList = allTokensSize - tokens.size(); // size of list of all tokens - where we are now
        tokens.remove(0); // remove where token
        //remove condition ; condition guarenteed to have 3 tokens
        ArrayList<Object> whereList = new ArrayList<Object>();

        //bar = 10
        while (!tokens.get(0).equals(";") && !tokens.get(0).equals("orderby")){
            ArrayList<String> curColumn = new ArrayList<>();
            String tableAndColumn = tokens.remove(0);
            curIndexInTokensList++;
            if(tableAndColumn.contains(".")){ // foo.x
                String[] tableAndColumnTokens = tableAndColumn.split("\\.");
                String table = tableAndColumnTokens[0];
                String column = tableAndColumnTokens[1];
                curColumn.add(table);
                curColumn.add(column);
            } else { // table and column is really just a column like x
                String column = tableAndColumn;
                curColumn.add(null);
                curColumn.add(column);
            }

            String relOp = tokens.remove(0); // > <= != =
            curIndexInTokensList++;
            String valueString = tokens.remove(0);
            curIndexInTokensList++;
            Object value = null;
            ArrayList<String> curColumn2 = new ArrayList<>();

            // HERE WE NEED TO CHECK IF THE RIGHT VALUE IS A VALUE OR IF IT IS A TABLE AND COLUMN CAN BE BOTH
            if(!setOfIndicesInCurrentStringCommandListWithString.contains(curIndexInTokensList)){ // this will check if the value is a table and a column or a value
                boolean isDouble = false;
                try{
                    Double d = Double.parseDouble(valueString);
                    isDouble = true;
                }catch (Exception e){
                    isDouble = false;
                }
                if(valueString.contains(".") && !isDouble){ // foo.x
                    String[] tableAndColumnTokens = valueString.split("\\.");
                    String table = tableAndColumnTokens[0];
                    String column = tableAndColumnTokens[1];
                    curColumn2.add(table);
                    curColumn2.add(column);
                    value = curColumn2;
                } else { // table and column is really just a column like x
                    value = parseValue(valueString);
                    if(value instanceof String){ // we have a column with no table attached to it
                        curColumn2.add(null);
                        curColumn2.add((String) value);
                        value = curColumn2;
                    }
                }
            } else {
                value = valueString;
            }
            whereList.add(curColumn);
            whereList.add(relOp);
            whereList.add(value);
            if(tokens.get(0).equals(KeywordType.AND.toString())|| tokens.get(0).equals(KeywordType.OR.toString())){
                whereList.add(tokens.remove(0));
                curIndexInTokensList++;
            }
        }
        return whereList;
    }


    /**
     * This abstract method is uniquely implemented by the child Command.
     * and occurs when a command needs to be parsed into its proper components.
     * <p>
     * returns boolean on success or failure
     */
    public abstract Boolean parseCommand();

}
