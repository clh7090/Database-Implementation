package queryProcessor.commands;

import queryProcessor.whereTree.WhereTree;

import java.util.ArrayList;
import java.util.HashSet;

public class DeleteCommand extends Command{
    private WhereTree whereTree;

    // used to figure out where strings are and used to see if the right side of a clause is a string or column
    private HashSet<Integer> setOfIndicesInCurrentStringCommandListWithString;

    public DeleteCommand(ArrayList<String> tokens, HashSet<Integer> setOfIndicesInCurrentStringCommandListWithString) {
        super(tokens);
        this.whereTree = null; // if this stays null we delete all rows in table
        this.setOfIndicesInCurrentStringCommandListWithString = setOfIndicesInCurrentStringCommandListWithString;
    }


    /**
     * delete from foo;
     * delete from foo where bar = 10;
     * delete from foo where bar > 10 and foo = "baz";
     * delete from foo where bar != bazzle;
     */
    @Override
    public Boolean parseCommand() {
        int tokensSize = tokens.size();
        tokens.remove(0);
        tokens.remove(0); // get rid of delete from
        extractName();
        if(tokens.get(0).equals(";")){ // no where clause
            return true; // we are done
        }
        ArrayList<Object> whereList = extractWhereList(setOfIndicesInCurrentStringCommandListWithString, tokensSize); // otherwise we need to extract the where clause and put it into a tree
        whereTree = new WhereTree(whereList);
        return true;
    }

    public String getName() {
        return name;
    }

    public WhereTree getWhereTree() {
        return whereTree;
    }

}
