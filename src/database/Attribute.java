package database;

import java.util.ArrayList;

/**
 * A class used for holding an attribute stored in a table.
 * This class is only used to store data for a command; it is not
 * for the actual table class that is separate in table.
 */
public class Attribute {
    private String name;

    private ArrayList<String> type; // if int, bool, double ignore second arg, else char/varchar use it
    // [Integer, 0], [char, 10], [bool,0] 

    private int isPrimaryKey; // 1 if key, 0 if not

    private boolean isNotNull;

    private boolean isUnique;

    public Attribute(String name, ArrayList<String> type, int isPrimaryKey, boolean isNotNull, boolean isUnique) {
        this.name = name;
        this.type = type;
        this.isPrimaryKey = isPrimaryKey;
        this.isNotNull = isNotNull;
        this.isUnique = isUnique;
    }


    public String getName() {
        return name;
    }


    public ArrayList<String> getType() {
        return type;
    }


    public int isPrimaryKey() {
        return isPrimaryKey;
    }


    public String typeString() {
        if(type.get(1) == null){
            return type.get(0);
        }
        return type.get(0) + "(" + String.valueOf(type.get(1)) + ")";
    }

    @Override
    public String toString() {
        if(type.get(1) == null){
            return name + " " + type.get(0);
        }
        return name + " " + type.get(0) + " " + type.get(1);
    }


    public boolean isNotNull() {
        return isNotNull;
    }

    public boolean isUnique() {
        return isUnique;
    }
}
