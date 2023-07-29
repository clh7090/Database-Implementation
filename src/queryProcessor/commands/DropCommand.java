package queryProcessor.commands;

import java.util.ArrayList;

public class DropCommand extends Command{

    public DropCommand(ArrayList<String> tokens) {
        super(tokens);
    }


    /**
     * Parses string from user input into form
     * <p>
     * drop table <name>;
     * <name> is the name of the table. Table names are unique in the system.
     */
    @Override
    public Boolean parseCommand() {
        // exception is not handled here if table dne need to check db
        tokens.remove(0);
        tokens.remove(0);
        extractName();
        return true;
    }


    public String getName() {
        return name;
    }

}
