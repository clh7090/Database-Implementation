package queryProcessor.commands;

import queryProcessor.whereTree.WhereTree;

import java.util.ArrayList;
import java.util.HashSet;

public class UpdateCommand extends Command{

    private String updateColumn;

    private Object updateValue;

    private WhereTree whereTree;

    // used to figure out where strings are and used to see if the right side of a clause is a string or column
    private HashSet<Integer> setOfIndicesInCurrentStringCommandListWithString;

    public UpdateCommand(ArrayList<String> tokens, HashSet<Integer> setOfIndicesInCurrentStringCommandListWithString) {
        super(tokens);
        this.updateColumn = null;
        this.updateValue = null;
        this.whereTree = null;
        this.setOfIndicesInCurrentStringCommandListWithString = setOfIndicesInCurrentStringCommandListWithString;
    }


    /**
     * update foo set bar = 5 where baz < 3.2;
     * update foo set bar = 1.1 where a = "foo" and bar > 2;
     */
    @Override
    public Boolean parseCommand() {
        int tokensSize = tokens.size();
        tokens.remove(0); // get rid of update token
        extractName();
        tokens.remove(0); // get rid of set token
        extractUpdateColumnAndValue();
        ArrayList<Object> whereList = extractWhereList(setOfIndicesInCurrentStringCommandListWithString, tokensSize); // otherwise we need to extract the where clause and put it into a tree
        whereTree = new WhereTree(whereList);
        return true;
    }


    private void extractUpdateColumnAndValue(){
        updateColumn = tokens.remove(0);
        tokens.remove(0); // get rid of = token in column = value
        updateValue = tokens.remove(0);
    }


    public String getName() {
        return name;
    }


    public Object getUpdateValue() {
        return updateValue;
    }


    public String getUpdateColumn() {
        return updateColumn;
    }


    public WhereTree getWhereTree() {
        return whereTree;
    }

}
