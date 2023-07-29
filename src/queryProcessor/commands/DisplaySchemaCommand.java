package queryProcessor.commands;

import java.util.ArrayList;

public class DisplaySchemaCommand extends Command {

    public DisplaySchemaCommand(ArrayList<String> tokens) {
        super(tokens);
    }


    /**
     * Parses string from user input into form
     * <p>
     * display schema;
     */
    public Boolean parseCommand() {
        // this command really does nothing until we query
        return true;
    }

}
