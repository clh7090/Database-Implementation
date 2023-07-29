package queryProcessor;

import database.Catalog;
import database.Database;
import database.PageBuffer;
import database.StorageManager;
import queryProcessor.commands.*;
import queryProcessor.queries.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Connor Hunter
 * <p>
 * A class made for grabbing input from the user
 * and turning it into SQL queries.
 * <p>
 * This command line interface is made to be
 * a part of the query processor.
 */
public class CLI {

    private boolean running;

    private Command currentCommand;


    private String commandType;

    private Query currentQuery;

    private Scanner sc;

    private Catalog catalog;

    private Database database;

    public CLI(Catalog catalog, Database database) {
        this.running = true;
        this.currentCommand = null;
        this.commandType = null;
        this.currentQuery = null;
        this.sc = new Scanner(System.in);
        this.catalog = catalog;
        this.database = database;
    }


    private HashSet<Integer> getSetOfIndicesInCurrentStringCommandListWithString(String currentStringCommand){
        // this code is for getting the indexes of the strings returned list
        currentStringCommand.trim();
        int idx = 0;
        boolean isOpeningQuote = true;
        boolean isInsideQuote = false;
        HashSet<Integer> set = new HashSet<>();
        for (char c : currentStringCommand.toCharArray()){
            if (c == '\"'){
                if(isOpeningQuote){
                    set.add(idx); // there is a quote at this idx of the command list i.e this token is a char/varchar
                    isOpeningQuote = false;
                    isInsideQuote = true;
                }else {
                    isOpeningQuote = true;
                    isInsideQuote = false;
                }
            } else if ((!isInsideQuote) && c == ' ' || c == ',' || c == '(' || c == ')' || c == ';') { // we are at a new index
                idx++;
            }
        }
        return set;
    }

    /**
     * Generates command tokens that are split by space and ignore spaces in quotes
     * it also gets rid of tokens that are just empty quotes since they were just spaces.
     *
     * @param currentStringCommand command String
     * @return Array List of command tokens
     */
    private static ArrayList<String> generateCurrentStringCommandList(String currentStringCommand) {
        // first we need to split tokens by spaces(not in quotes) and chop off
        // extra whitespace tokens.

        ArrayList<String> list = new ArrayList<String>();
        Pattern regex = Pattern.compile("[^\\s\"']+|\"([^\"]*)\"|'([^']*)'");
        Matcher matcher = regex.matcher(currentStringCommand);
        while (matcher.find()) {
            if (matcher.group(1) != null) {
                list.add(matcher.group(1));
            } else if (matcher.group(2) != null) {
                list.add(matcher.group(2));
            } else {
                list.add(matcher.group());
            }
        }


        // next we need to put certain special chars in own token
        // such as , ( ) ; and " to make parsing easier, these can be
        // connected to other words/tokens. " is special since
        // it can be touching other args for a query if something
        // is in quotes, we still need to separate it.
        // these are account for above in regex and quoted strings
        // are already split into own words.

        for (int i = 0; i < list.size(); i++) {
            // insert char behind current token
            if ((list.get(i).startsWith(",") || list.get(i).startsWith("(")
                    || list.get(i).startsWith(")")
                    || list.get(i).startsWith(";")) && list.get(i).length() != 1) {
                String newCharToSplit = null;
                String oldCharToSplit = list.get(i).substring(0, 1);
                if (oldCharToSplit.equals("(")) {
                    newCharToSplit = "\\(";
                }
                if (oldCharToSplit.equals(")")) {
                    newCharToSplit = "\\)";
                }
                String[] charAndToken =
                        list.get(i).split(newCharToSplit == null ? oldCharToSplit : newCharToSplit, 2);
                if (charAndToken[0].equals("")) {
                    list.add(i, oldCharToSplit);
                } else {
                    list.add(i, charAndToken[0]);
                }
                list.set(i + 1, charAndToken[1]);

                i = 0; //restart the loop
                continue;
            }
            // insert char ahead current token
            if ((list.get(i).endsWith(",") || list.get(i).endsWith("(")
                    || list.get(i).endsWith(")")
                    || list.get(i).endsWith(";")) && list.get(i).length() != 1) {
                String charToSplit = list.get(i).substring(list.get(i).length() - 1);
                String[] tokenAndChar = {list.get(i).substring(0, list.get(i).length() - 1), charToSplit};
                list.set(i, tokenAndChar[0]);
                list.add(i + 1, tokenAndChar[1]);


                i = 0; //restart the loop
                continue;
            }

            // split current token into 3 happens when token inbetween 2 others needs to be split
            if ((list.get(i).contains(",") || list.get(i).contains("(")
                    || list.get(i).contains(")")
                    || list.get(i).contains(";")) && list.get(i).length() != 1) {
                String newCharToSplit = null;
                String oldCharToSplit = null;
                for (char c : list.get(i).toCharArray()) {
                    if (c == ',' || c == '(' || c == ')' || c == ';') {
                        oldCharToSplit = String.valueOf(c);
                    }
                }
                if (oldCharToSplit.equals("(")) {
                    newCharToSplit = "\\(";
                }
                if (oldCharToSplit.equals(")")) {
                    newCharToSplit = "\\)";
                }
                String[] tokenAndChar =
                        list.get(i).split(newCharToSplit == null ? oldCharToSplit : newCharToSplit, 2);
                list.set(i, tokenAndChar[0]);
                list.add(i + 1, oldCharToSplit);
                list.add(i + 2, tokenAndChar[1]);
                i = 0; //restart the loop
                continue;
            }
        }


        // next, we need to get all of the keywords, and make sure they
        // are all lowercase

        for (int i = 0; i < list.size(); i++) {
            for (KeywordType keywordType : KeywordType.values()) {
                if (list.get(i).toLowerCase().equals(keywordType.toString())) {
                    list.set(i, list.get(i).toLowerCase());
                }
            }
        }

        return list;
    }

    /**
     * This method is for determining what type of Command a command is.
     * It determines this by tokenizing a user given string.
     *
     * @return the String command type.
     */
    private static String determineCommandType(ArrayList<String> stringCommandList) {
        switch (stringCommandList.get(0).toLowerCase()) {
            case "quit":
                return "quit";
            case "create":
                return "create";
            case "select":
                return "select";
            case "insert":
                return "insert";
            case "alter":
                return "alter";
            case "drop":
                return "drop";
            case "update":
                return "update";
            case "delete":
                return "delete";
        }
        switch (stringCommandList.get(1).toLowerCase()) {
            case "schema":
                return "schema";
            case "info":
                return "info";
        }
        return null;
    }

    /**
     * This method deals with starting up the CLI and
     * handling the queries given. If the user types
     * 'quit' the query processor will gracefully shut down.
     */
    public void startQueryProcessor() {
        System.out.println("Please enter commands, enter <quit> to shutdown the db");
        Boolean commandSuccess;
        boolean querySuccess;
        while (running) {
            generateCommand();
            commandSuccess = currentCommand.parseCommand();
            if (commandSuccess == null) continue; // quit command
            if (commandSuccess == false) continue; // bad command get next
            generateQuery();
            querySuccess = currentQuery.handleQuery();
            if (commandSuccess && querySuccess) System.out.println("SUCCESS");
        }
        sc.close();
        endQueryProcessor();
    }

    /**
     * This Method is for generating a proper command given
     * a string of user input.
     */
    private void generateCommand() {
        String currentStringCommand = promptUserForCommand();
        ArrayList<String> currentStringCommandList;
        HashSet<Integer> setOfIndiciesInCurrentStringCommandListWithString = getSetOfIndicesInCurrentStringCommandListWithString(currentStringCommand);
        currentStringCommandList = generateCurrentStringCommandList(currentStringCommand);
        commandType = determineCommandType(currentStringCommandList);
        switch (commandType) {
            case "quit":
                currentCommand = new QuitCommand(currentStringCommandList);
                running = false; // stop query processor if this happens
                break;
            case "create":
                currentCommand = new CreateTableCommand(currentStringCommandList);
                break;
            case "select":
                currentCommand = new SelectCommand(currentStringCommandList, setOfIndiciesInCurrentStringCommandListWithString);
                break;
            case "insert":
                currentCommand = new InsertCommand(currentStringCommandList);
                break;
            case "schema":
                currentCommand = new DisplaySchemaCommand(currentStringCommandList);
                break;
            case "info":
                currentCommand = new DisplayInfoCommand(currentStringCommandList);
                break;
            case "alter":
                currentCommand = new AlterTableCommand(currentStringCommandList);
                break;
            case "drop":
                currentCommand = new DropCommand(currentStringCommandList);
                break;
            case "update":
                currentCommand = new UpdateCommand(currentStringCommandList, setOfIndiciesInCurrentStringCommandListWithString);
                break;
            case "delete":
                currentCommand = new DeleteCommand(currentStringCommandList, setOfIndiciesInCurrentStringCommandListWithString);
                break;
        }
    }

    /**
     * This method is for getting user input
     *
     * @return
     */
    private String promptUserForCommand() {
        StringBuilder sb = new StringBuilder();
        String currentLine = "";
        while (!currentLine.endsWith(";") && !currentLine.equalsIgnoreCase("quit")) {
            currentLine = sc.nextLine();
            sb.append(currentLine);
            if (!currentLine.endsWith(";")) {
                sb.append(" ");
            }
        }
        // return string with no leading or trailing whitespace ending with ;
        return sb.toString().trim();
    }

    /**
     * This method is for generating an SQL query given that there is a
     * user given command given.
     */
    private void generateQuery() {
        switch (commandType) {
            case "create":
                currentQuery = new CreateTableQuery((CreateTableCommand) currentCommand, database);
                break;
            case "select":
                currentQuery = new SelectQuery((SelectCommand) currentCommand, database);
                break;
            case "insert":
                currentQuery = new InsertQuery((InsertCommand) currentCommand, database);
                break;
            case "schema":
                currentQuery = new DisplaySchemaQuery((DisplaySchemaCommand) currentCommand, database);
                break;
            case "info":
                currentQuery = new DisplayInfoQuery((DisplayInfoCommand) currentCommand, database);
                break;
            case "alter":
                currentQuery = new AlterTableQuery((AlterTableCommand) currentCommand, database);
                break;
            case "drop":
                currentQuery = new DropQuery((DropCommand) currentCommand, database);
                break;
            case "update":
                currentQuery = new UpdateQuery((UpdateCommand) currentCommand, database);
                break;
            case "delete":
                currentQuery = new DeleteQuery((DeleteCommand) currentCommand, database);
                break;
        }
    }


    /**
     * This method is for gracefully stopping the query processor.
     */
    private void endQueryProcessor() {
        System.out.println("Safely shutting down the database...");
        System.out.println("Purging page buffer...");
        System.out.println("Saving catalog...");

        // gets catalog from database and writes it to file
        try {
            // giving catalog access to tables to record pageOrder
            database.getCatalog().extractTables(database.getTables());
            database.getCatalog().writeCatalog();
            StorageManager manager = database.getBuffer();
            // EDGE CASE: if database is created and closed with no records, num of pages will not
            // be written and when restarted it will try to read something that is not there
            PageBuffer pagebuff  = manager.getBuffer();
            pagebuff.purgeBuffer();
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println(
                "\n" +
                "Exiting the database...");
    }
}
