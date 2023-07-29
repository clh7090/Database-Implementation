import database.Catalog;
import database.Database;
import queryProcessor.CLI;

import java.io.File;
import java.io.IOException;

public class Main {


    /**
     * The main driver program
     *
     * @param args cmd line args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        String dbLoc = args[0];

        Integer pageSize = Integer.parseInt(args[1]);
        Integer bufferSize = Integer.parseInt(args[2]);

        boolean indexflag = false;
        Database database;
        System.out.println(String.format("Looking at %s for existing db....", dbLoc));
        boolean databaseExists = checkDatabaseExists(dbLoc);
        Catalog catalog = new Catalog(dbLoc, pageSize);
        if(args.length != 4){
            System.out.println("Indexing OFF\n");
        }
        else{
            boolean indexon = args[3].equalsIgnoreCase("true");
            if(indexon){
                System.out.println("Indexing ON\n");
                indexflag = true;
            }
            else{
                System.out.println("Indexing OFF\n");
            }
        }
        if (databaseExists) {
            System.out.println("Database found...\n" +
                    "Restarting the database...\n" +
                    "    Ignoring provided pages size, using stored page size");

            // reads catalog file and restores it
            catalog.restoreSchema();
            int oldPageSize = catalog.getPageSize();

            // TODO: reads table files and restores them


            // puts restored catalog and tables into new database
            // TODO: put restored tables in restoreDatabase function

            database = new Database(dbLoc, oldPageSize, bufferSize, catalog, indexflag);
            database.restoreDatabase(catalog);

            System.out.println("Database restarted successfully");
            System.out.println(String.format("Page Size: %d", oldPageSize));

        } else {
            System.out.println("No existing db found");
            System.out.println(String.format("Creating new db at %s", dbLoc));
            database = new Database(dbLoc, pageSize, bufferSize, indexflag);
            System.out.println("New db created successfully");
            System.out.println(String.format("Page Size: %d", pageSize));
        }

        System.out.println(String.format("Buffer Size: %d", bufferSize));

        CLI cli = new CLI(catalog, database);
        cli.startQueryProcessor();
    }

    private static boolean checkDatabaseExists(String filepath) {
            File file = new File(filepath + "\\Catalog");
            return file.exists();
    }
}