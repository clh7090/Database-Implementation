package queryProcessor.commands;

import java.util.ArrayList;

public class DisplayInfoCommand extends Command {

    public DisplayInfoCommand(ArrayList<String> tokens) {
        super(tokens);
    }


    /**
     * Parses string from user input into form
     * <p>
     * display info <name>;.
     *
     * <name> is the name of the table.
     */
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
