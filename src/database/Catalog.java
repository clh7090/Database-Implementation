package database;

import exception.*;
import queryProcessor.queries.CreateTableQuery;
import queryProcessor.queries.InsertQuery;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class Catalog {

    private HashMap<String, SchemaTable> schema;
    private HashMap<Integer, String> tableName;
    private int pageSize;

    private int numTables;


    // used to extract pageorder for writing
    private HashMap<String, Table> tables;

    // file path for catalog
    private String filePath;


    /**
     * Creates the hash-map for all table schema
     */
    public Catalog(String dbLoc, int pageSize) {
        schema = new HashMap<>();
        tableName = new HashMap<>();
        tables = new HashMap<>();
        filePath = dbLoc + "\\Catalog";
        this.pageSize = pageSize;
    }

    /**
     * writes catalog to file
     * @throws IOException
     */
    public void writeCatalog() throws IOException {
        RandomAccessFile raf = new RandomAccessFile(new File(filePath), "rw");
        raf.seek(0);

        //number of tables;
        raf.writeInt(getNumTables());
        // page size
        raf.writeInt(pageSize);

        // writes tables
        writeTables(raf, getNumTables());

        raf.close();
    }

    /**
     * writes tables to file
     * @param raf
     * @param numTables
     * @throws IOException
     */
    private void writeTables(RandomAccessFile raf, int numTables) throws IOException {
        // reorders tables by tableID and puts them into a list
        ArrayList<SchemaTable> schemaTableList = new ArrayList<>(numTables);
        for (int i = 0; i < numTables; i++) {
            schemaTableList.add(null);
        }
        for (Map.Entry<String, SchemaTable> table : schema.entrySet()) {
            SchemaTable schemaTable = table.getValue();
            int tableID = schemaTable.getTableID();

            schemaTableList.set(tableID, schemaTable);
        }

        // write tables to catalog file
        for (int tableID = 0; tableID < numTables; tableID++) {
            SchemaTable schemaTable = schemaTableList.get(tableID);

            // table ID
            raf.writeInt(tableID);

            // table name
            writeChars(raf, schemaTable.getTableName());

            // number of attributes
            int numAttrs = schemaTable.getAttributes().size();
            raf.writeInt(numAttrs);

            // writes attributes
            writeAttributes(raf, schemaTable.getAttributes());

            // writes pageID order
            writePageOrder(raf, schemaTable.getTableName(), tables);
        }

    }

    /**
     * writes number of chars and the chars to file
     * @param raf
     * @param str
     * @throws IOException
     */
    private void writeChars(RandomAccessFile raf, String str) throws IOException {
        // write string length
        int len = str.length();
        raf.writeInt(len);

        // write chars
        char[] chars = str.toCharArray();
        for (int i = 0; i < len; i++) {
            raf.writeChar(chars[i]);
        }
    }

    /**
     * writes attributes to file
     * @param raf
     * @param attributes
     * @throws IOException
     */
    private void writeAttributes(RandomAccessFile raf, ArrayList<Attribute> attributes) throws IOException {
        // iterate through and write attributes to file
        for (Attribute attr : attributes) {
            // attribute name
            String attrName = attr.getName();
            writeChars(raf, attrName);

            // is primary key?
            raf.writeInt(attr.isPrimaryKey());

            if(attr.isUnique()){
                raf.writeInt(1);
            }else {
                raf.writeInt(0);
            }

            if(attr.isNotNull()){
                raf.writeInt(1);
            }else {
                raf.writeInt(0);
            }

            ArrayList<String> attrType = attr.getType();

            // type
            int typeKey = getTypeKey(attrType.get(0));
            raf.writeInt(typeKey);

            // number of characters (for char and varchar)
            String numChars = attrType.get(1);
            if (numChars==null) { numChars = "0"; }
            raf.writeInt(Integer.parseInt(numChars));
        }
    }

    private void writePageOrder(RandomAccessFile raf, String tableName, HashMap<String, Table> tables) throws IOException {
        Table t = tables.get(tableName);
        int num = t.getNumPages();
        ArrayList<Integer> numPages = t.getPages();
        raf.writeInt(num);
        for(int i = 0; i < num; i++) {
            raf.writeInt(numPages.get(i));
        }
    }

    /**
     * returns int key based on string of type
     * @param typeStr
     * @return
     */
    public int getTypeKey(String typeStr) {
        typeStr = typeStr.toLowerCase();
        switch (typeStr) {
            case "integer":
                return 0;
            case "double":
                return  1;
            case "boolean":
                return  2;
            case "char":
                return  3;
            case "varchar":
                return 4;
        }
        return 0;
    }

    /**
     * returns type string based on key
     * @param typeKey
     * @return
     */
    private String getTypeString(int typeKey) {

        switch (typeKey) {
            case 0:
                return "integer";
            case 1:
                return  "double";
            case 2:
                return  "boolean";
            case 3:
                return  "char";
            case 4:
                return "varchar";
        }
        return "integer";
    }

    /**
     * Restore table schema from memory
     * @return
     */
    public void restoreSchema() throws IOException {
        // reading catalog file to recreate database
        RandomAccessFile raf = new RandomAccessFile(new File(filePath), "r");
        raf.seek(0);
        numTables = raf.readInt();
        pageSize = raf.readInt();

        // add tables to schema
        readTables(raf, numTables);

        raf.close();
    }

    /**
     * reads and returns string based on length given
     * @param raf
     * @param len
     * @return
     * @throws IOException
     */
    private String readChars(RandomAccessFile raf, int len) throws IOException {
        String str = "";
        for (int i = 0; i < len; i++) {
            str += raf.readChar();
        }
        return str;
    }

    /**
     * reads tables from catalog and adds them to schema
     * @param raf
     * @param numTables
     * @throws IOException
     */
    private void readTables(RandomAccessFile raf, int numTables) throws IOException {
        // reading table schemas
        for(int i = 0; i < numTables; i++) {
            int tableID = raf.readInt();
            int nameLen = raf.readInt();
            String tableName = "";

            tableName = readChars(raf, nameLen);

            int numAttrs = raf.readInt();

            // reads attributes
            ArrayList<Attribute> attributes = readAttributes(raf, numAttrs);

            // read page order
            ArrayList<Integer> pageOrder = new ArrayList<>();
            int pageOrderLen = raf.readInt();
            for(int j = 0; j < pageOrderLen; j++) {
                pageOrder.add(raf.readInt());
            }

            // add table to schema
            schema.put(tableName, new SchemaTable(tableID, tableName, attributes, pageOrder));
            this.tableName.put(tableID, tableName);
        }
    }

    /**
     * reads and returns the list of attributes from the catalog file
     * @param raf
     * @param numAttrs
     * @throws IOException
     */
    private ArrayList<Attribute> readAttributes(RandomAccessFile raf, int numAttrs) throws IOException {
        boolean isUnique;
        boolean isNotNull;
        ArrayList<Attribute> attributes = new ArrayList<>();

        for (int z = 0; z < numAttrs; z++) {
            int attrNameLen = raf.readInt();
            String attrName = "";

            attrName = readChars(raf, attrNameLen);

            int pKey = raf.readInt();
            int unique = raf.readInt();
            if(unique==1){
                isUnique = true;
            }else {
                isUnique = false;
            }
            int notnull = raf.readInt();
            if(notnull==1){
                isNotNull = true;
            }else {
                isNotNull = false;
            }
            int attrType = raf.readInt();
            int attrSize = raf.readInt();

            // sets type array
            ArrayList<String> attrTypeArr = new ArrayList<>(2);
            attrTypeArr.add(getTypeString(attrType));
            attrTypeArr.add(String.valueOf(attrSize));

            // adds attribute to attribute list
            Attribute attr = new Attribute(attrName, attrTypeArr, pKey, isNotNull, isUnique);
            attributes.add(attr);
        }

        return attributes;
    }

    public int getPageSize() {
        return  pageSize;
    }

    /**
     * Creates an individual table schema
     * returns ID it uses for table
     */
    public int createSchema(String name, ArrayList<Attribute> attributes) {

        int nextID = schema.size();
        tableName.put(nextID, name);
        schema.put(name, new SchemaTable(nextID, name, attributes, new ArrayList<>()));
        return nextID;
    }

    public void verifyInsertValue(String name, ArrayList<Object> row) {
        ArrayList<Attribute> insertValueTypes = new ArrayList<>();
        ArrayList<Attribute> validSchema = schema.get(name).getAttributes();
        for (int i = 0; i < row.size(); i++) {
            ArrayList dummyTypeList = new ArrayList();
            if (row.get(i) instanceof Integer) {
                dummyTypeList.add("integer");
                dummyTypeList.add(null);
            } else if (row.get(i) instanceof Double) {
                dummyTypeList.add("double");
                dummyTypeList.add(null);
            } else if (row.get(i) instanceof Boolean) {
                dummyTypeList.add("boolean");
                dummyTypeList.add(null);
            } else if (row.get(i) instanceof String) {
                dummyTypeList.add("char"); // also case for varchar youre given a char
                dummyTypeList.add(((String) row.get(i)).length());
            } else { // value is null in this [row][col]
                dummyTypeList.add(null);
                dummyTypeList.add(null);
            }
            insertValueTypes.add(new Attribute("value", dummyTypeList, 0, false, false));
        }

        // calculate expected string
        String expectedString = "";
        StringBuilder builder = new StringBuilder();
        for (Attribute attribute : validSchema) {
            builder.append(attribute.typeString() + " ");
        }
        expectedString = builder.toString().substring(0,builder.toString().length() -1); // get rid of last space
        builder = new StringBuilder();
        ArrayList<String> gotStringList = new ArrayList<>();

        //calculate got string List
        for (int j = 0; j < insertValueTypes.size(); j++) {
            if(j == row.size()-1){
                builder.append(insertValueTypes.get(j).typeString());
            }else {
                builder.append(insertValueTypes.get(j).typeString() + " ");
            }
        }
        gotStringList.add(builder.toString());

        for (int j = 0; j < row.size(); j++) { // foreach value in the row
            if (row.size() != validSchema.size()) {
                throw new IncorrectAmountOfAttributesInRowException(String.format
                        ("Error: INVALID AMOUNT OF VALUES GIVEN: expected (%s) got (%s)", expectedString, gotStringList.get(j)));
            }
            if (row.get(j) == null) { // okay in this method, check not null in different method
                continue;
            } else if ((row.get(j) instanceof Integer && !validSchema.get(j).getType().get(0).equals("integer") ) ) {
                throw new InvalidDataTypeInsertionException(
                        String.format("INVALID DATA TYPE: expected (%s) got (%s)", expectedString, gotStringList.get(j)));
            } else if (row.get(j) instanceof Double && !validSchema.get(j).getType().get(0).equals("double")) {
                throw new InvalidDataTypeInsertionException(
                        String.format("INVALID DATA TYPE: expected (%s) got (%s)", expectedString, gotStringList.get(j)));
            } else if (row.get(j) instanceof Boolean && !validSchema.get(j).getType().get(0).equals("boolean")) {
                throw new InvalidDataTypeInsertionException(
                        String.format("INVALID DATA TYPE: expected (%s) got (%s)", expectedString, gotStringList.get(j)));
            } else if (row.get(j) instanceof String && (!validSchema.get(j).getType().get(0).equals("char") &&
                    !validSchema.get(j).getType().get(0).equals("varchar"))) {
                throw new InvalidDataTypeInsertionException(
                        String.format("INVALID DATA TYPE: expected (%s) got (%s)", expectedString, gotStringList.get(j)));
            } else if (row.get(j) instanceof String && validSchema.get(j).getType().get(0).equals("char")) {
                if (((String) row.get(j)).length() != Integer.parseInt(validSchema.get(j).getType().get(1))) {
                    throw new CharLengthInsertValueIncorrectException(
                            String.format("ERROR %s can only accept %s chars, %s is %s", validSchema.get(j).typeString(),
                                    validSchema.get(j).getType().get(1), row.get(j), ((String) row.get(j)).length()));
                }
            } else if (row.get(j) instanceof String && validSchema.get(j).getType().get(0).equals("varchar")) {
                if (((String) row.get(j)).length() > Integer.parseInt(validSchema.get(j).getType().get(1))) {
                    throw new VarcharLengthExceededException(
                            String.format("ERROR %s can only accept up to %s chars. %s is more than %s chars", validSchema.get(j).typeString(),
                                    validSchema.get(j).getType().get(1), row.get(j), validSchema.get(j).getType().get(1)));
                }
            }
        }
    }

    public ArrayList<Attribute> getSchemaGivenTableName(String name) {
        return schema.get(name).getAttributes();
    }

    public void checkTableExists(String name, boolean tableShouldExist) {
        if (tableShouldExist && !schema.containsKey(name)) {
            throw new NoSuchTableException(String.format("ERROR: TABLE %s DOES NOT EXIST", name));
        } else if (!tableShouldExist && schema.containsKey(name)) {
            throw new TableAlreadyExistsException(String.format("ERROR: TABLE %s ALREADY EXISTS", name));
        }
    }

    // throws error if user tries to drop attribute that's a primary key
    public void checkDropPrimaryKey(String tableName, String attrName) {
        ArrayList<Attribute> attrs = schema.get(tableName).getAttributes();
        for (Attribute attr : attrs) {
            if (attr.getName().equals(attrName)) {
                if (attr.isPrimaryKey() == 1) {
                    throw new AlterTableDropPrimaryKeyException(String.format("CANNOT DROP PRIMARY KEY %s", attrName));
                }
                else {
                    return;
                }
            }
        }
    }

    public void checkAddDuplicateAttr(String tableName, String attrName) {
        ArrayList<Attribute> attrs = schema.get(tableName).getAttributes();
        for (Attribute attr : attrs) {
            if (attrName.equals(attr.getName())) {
                throw new DuplicateAttributeNameException(String.format("DUPLICATE ATTRIBUTE NAMES NOT ALLOWED: attribute name: %s", attrName));
            }
        }
    }

    public String getTableName(int tableID){
        return tableName.get(tableID);
    }

    public HashMap<Integer, String> getTableNameSchema(){return tableName;}

    public int getNumTables(){return schema.size();}

    public HashMap<String, SchemaTable> getSchema() {
        return schema;
    }

    // if returns true, r1 should go before r2. If false r1 is larger than r2
    public boolean compareKeys(SchemaTable sTable, Record r1, Record r2) {
        int keyIdx = 0;
        ArrayList<String> type = new ArrayList<>();
        ArrayList<Attribute> attrs = sTable.getAttributes();
        // finding pkey index
        for(int i = 0; i < attrs.size(); i++) {
            Attribute curAttr = attrs.get(i);
            if(curAttr.isPrimaryKey() == 1) {
                keyIdx = i;
                type = curAttr.getType();
                break;
            }
        }

        // casting to appropriate type and comparing
        switch(type.get(keyIdx)) {
            case "integer":
                if((int)r1.getData().get(keyIdx) > (int)r2.getData().get(keyIdx)) {
                    return false;
                }
                return true;
            case "double":
                if((Double)r1.getData().get(keyIdx) > (Double)r2.getData().get(keyIdx)) {
                    return false;
                }
                return true;
            case "char":
                if(r1.getData().get(keyIdx).toString().compareTo(r2.getData().get(keyIdx).toString()) < 0) {
                    return false;
                }
                return true;
            case "varchar":
                if(r1.getData().get(keyIdx).toString().compareTo(r2.getData().get(keyIdx).toString()) < 0) {
                    return false;
                }
                return true; 
        }
        return false; // should never reach here but java wont stop yelling at me
    }

    public void extractTables(HashMap<String, Table> tables) {
        this.tables = tables;
    }

    public HashMap<String, Table> getTables() {
        return tables;
    }
}
