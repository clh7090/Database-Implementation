package database;


import java.util.ArrayList;

public class Table {

    private ArrayList<Integer> pages;
    private int tableID;
    private String tableName;

    public Table(String name, int ID) {
        pages = new ArrayList<>();
        tableID= ID;
        tableName = name;
    }


    public int getNumPages() {
        return pages.size();
    }


    public ArrayList<Integer> getPages() {
        return pages;
    }


    public String getTableName() {
        return tableName;
    }

    public int getTableID() {
        return tableID;
    }
}
