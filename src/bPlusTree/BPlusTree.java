package bPlusTree;

import database.Attribute;

import java.util.ArrayList;

public class BPlusTree {

    private String BPlusTreeName;

    private int n;

    private int primaryKeyIndex;

    private String primaryKeyDataType;

    private BPlusNode root;


    /**
     * Creates a B+ tree. B+ object will have a name, primary key data type, record max number of pointers as well as
     *  min number of pointers
     *
     * @param name String name for B+ tree
     * @param pagesize pagesize
     * @param attributes list of record attributes. Used to find primary key attribute to know what data type it is
     */
    public BPlusTree(String name, int pagesize, ArrayList<Attribute> attributes){
        this.BPlusTreeName = name;
        this.primaryKeyIndex = primaryKeyIndex(attributes);
        Attribute primaryKey = attributes.get(primaryKeyIndex);
        ArrayList<String> primKeyDataTypePair = primaryKey.getType();
        this.primaryKeyDataType = primKeyDataTypePair.get(0);
        this.n = calculateN(pagesize, primKeyDataTypePair);
        this.root = new BPlusNode(true, true, true);
    }

    /**
     * Helper function that finds the attribute that is the primary key
     *
     * @param attributes looks through table schema to locate attribute that is primary key
     * @return index value of for attribute that is a primary key; 0 if primary key isn't found
     */
    public int primaryKeyIndex(ArrayList<Attribute> attributes){
        for(int a = 0; a < attributes.size(); a++){
            Attribute attribute = attributes.get(a);
            if(attribute.isPrimaryKey() == 1){
                return a;
            }
        }

        //is it an error if it gets here (no primary key?)
        return 0;
    }

    /**
     * A function that calculates the N value of the tree
     *
     * @param pagesize pagesize is divided by pair size (page pointer plus integer; see write-up) to caluclate maxpointer
     * @param dataType arraylist that stores data type and data type size (for varchar, char) of primary key
     * @return N of the tree
     */
    private int calculateN(int pagesize, ArrayList<String> dataType){
        int dataTypeSize = getDataTypeSize(dataType);

        //Page pointer is always an integer so 4 plus value of dataType (see write-up)
        int pairSize = Integer.BYTES + dataTypeSize;

        return pagesize/pairSize -1;
    }



    /**
     * Helper function calculates the search-key byte size
     *
     * @param typeStr String array whose first index is the string type and second index the size of the data type; second
     *                index used for char and varchar types
     * @return search-key byte size
     */
    private int getDataTypeSize(ArrayList<String> typeStr) {
        String type = typeStr.get(0).toLowerCase();
        switch (type) {
            case "integer":
                return Integer.BYTES;
            case "double":
                return  Double.BYTES;
            case "char":
            case "varchar":
                int len = Integer.parseInt(typeStr.get(1));
                return  len*Character.BYTES;
        }
        return -1;
    }

    /**
     * Helper function that prints b+ tree info (for debugging)
     */
    public void printBPlusTreeInfo(){
        System.out.println("B+ tree name: " + this.BPlusTreeName);
        System.out.println("N of the tree: " + this.n);
        System.out.println("Primary key data type for tree: " + this.primaryKeyDataType);
    }

    public String getName() {
        return BPlusTreeName;
    }

    public int getPKeyIdx(){
        return primaryKeyIndex;
    }

    public String getPKeyType() {
        return primaryKeyDataType;
    }

    public BPlusNode getRoot() {
        return root;
    }

    public int getN() {
        return n;
    }


    public void setRoot(BPlusNode root) {
        this.root = root;
    }
}
