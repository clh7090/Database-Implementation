package database;

import java.util.ArrayList;

public class SchemaTable {
    private int tableID;
    private String tableName;
    private ArrayList<Attribute> attributes;
    private ArrayList<Integer> pageOrder;

    public SchemaTable(int tableID, String tableName, ArrayList<Attribute> attributes, ArrayList<Integer> pageOrder) {
        this.tableID = tableID;
        this.tableName = tableName;
        this.attributes = attributes;
        this.pageOrder = pageOrder;

    }

    public ArrayList<Attribute> getAttributes() {
        return attributes;
    }

    public int getTableID() {
        return tableID;
    }

    public String getTableName() {
        return tableName;
    }

    public ArrayList<Integer> getPageOrder() {
        return pageOrder;
    }
}
