package database;

import bPlusTree.BPlusNode;
import bPlusTree.BPlusTree;
import exception.*;
import queryProcessor.queries.*;
import queryProcessor.whereTree.WhereTree;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

public class Database {

    private Catalog catalog;

    private final HashMap<String, Table> tables;

    // key is table/tree name, value is a list in format [pageNumber, index]
    private HashMap<String, ArrayList<Integer>> tableIndexes;

    // key is table/tree name, values is tree structure
    private HashMap<String, BPlusTree> trees;

    private String dbLoc;

    private int pageSize;

    private int bufferSize;

    private StorageManager manager;

    boolean indexflag;

    // for creating new db
    public Database(String dbLoc, int pageSize, int bufferSize, boolean indexflag) {
        // may need dbLoc page size and buffer size for more detailed exceptions
        this.catalog = new Catalog(dbLoc, pageSize);
        this.tables = new HashMap<>();
        this.tableIndexes = new HashMap<>();
        this.trees = new HashMap<>();
        this.dbLoc = dbLoc;
        this.pageSize = pageSize;
        this.bufferSize = bufferSize;
        this.indexflag = indexflag;
        manager = new StorageManager(this.catalog, pageSize, dbLoc, bufferSize);
    }

    // for restoring database
    public Database(String dbLoc, int pageSize, int bufferSize, Catalog catalog, boolean indexflag) {
        // may need dbLoc page size and buffer size for more detailed exceptions
        this.catalog = catalog;
        this.tables = new HashMap<>();
        this.trees = new HashMap<>();
        this.dbLoc = dbLoc;
        this.pageSize = pageSize;
        this.bufferSize = bufferSize;
        this.indexflag = indexflag;
        manager = new StorageManager(this.catalog, pageSize, dbLoc, bufferSize);
    }

    public boolean createTable(CreateTableQuery query) {
        String name = query.getName();
        ArrayList<Attribute> attributes = query.getAttributes();

        return createTableFunc(name, attributes);
    }

    private boolean createTableFunc(String name, ArrayList<Attribute> attributes) {
        boolean tableShouldExist = false;
        try {
            catalog.checkTableExists(name, tableShouldExist);
        } catch (TableAlreadyExistsException e) {
            System.err.println(e);
            return false;
        }

        int tableID = catalog.createSchema(name, attributes); // uses same ID generated for tableName HashMap
        // checks schema for which tableID isn't taken
        // int tableID = 0;
        // while (catalog.getTableNameSchema().containsKey(tableID)) {
        // tableID++;
        // }
        if(indexflag){
            BPlusTree bptree = new BPlusTree(name, this.pageSize, attributes);
            trees.put(name, bptree);
            bptree.printBPlusTreeInfo();
        }
        tables.put(name, new Table(name, tableID));
        return true; // on success no exception
    }

    public boolean dropTable(DropQuery query) {
        String tableName = query.getName();

        return dropTableFunc(tableName);
    }

    private boolean dropTableFunc(String tableName) {
        boolean tableShouldExist = true;
        try {
            catalog.checkTableExists(tableName, tableShouldExist);
        } catch (NoSuchTableException e) {
            System.err.println(e);
            return false;
        }

        // database remove from hashmap tables
        Table table = tables.remove(tableName);

        // Removes table and associated pages from pagebuffer
        manager.getBuffer().removeTable(tableName);

        // Removes schemaTable for dropped table.
        catalog.getSchema().remove(tableName);

        // Removes from get table name with id
        catalog.getTableNameSchema().remove(table.getTableID());

        // Removes from extracting pageorder for writing
        catalog.getTables().remove(tableName);

        // Delete table from disk
        boolean done = new File(String.format("%s\\%d", dbLoc, table.getTableID())).delete();

        return true;
    }

    /**
     * drop or add alter table statements
     * gets altered copies of all records from given table, drops old table, creates
     * new table and inserts altered records
     *
     * @param query
     * @return
     */
    public boolean alterTable(AlterTableQuery query) {
        String tableName = query.getName();
        String method = query.getMethod();
        Attribute attr = query.getAttribute();
        Object defaultVal = query.getDefaultValue();

        boolean tableShouldExist = true;
        try {
            catalog.checkTableExists(tableName, tableShouldExist);
        } catch (NoSuchTableException e) {
            System.err.println(e);
            return false;
        }

        // get information from old table
        ArrayList<Attribute> oldAttrs = catalog.getSchemaGivenTableName(tableName);
        Table oldTable = tables.get(tableName);

        // create copy of table with specified adjustments from query
        ArrayList<Attribute> newAttrs = new ArrayList<>();
        ArrayList<ArrayList<Object>> allRecords = new ArrayList<>();

        if (method.equals("drop")) {
            try {
                catalog.checkDropPrimaryKey(tableName, attr.getName());
            } catch (AlterTableDropPrimaryKeyException e) {
                System.err.println(e);
                return false;
            }

            // excludes given attribute from newAttrs
            for (Attribute oldAttr : oldAttrs) {
                if (!(oldAttr.getName().equals(attr.getName()))) {
                    newAttrs.add(oldAttr);
                }
            }

            // gets index of specified attribute
            int attrIdx = -1;
            ArrayList<Attribute> schemaAttrs = catalog.getSchema().get(tableName).getAttributes();
            for (int i = 0; i < schemaAttrs.size(); i++) {
                Attribute schemaAttr = schemaAttrs.get(i);
                if (schemaAttr.getName().equals(attr.getName())) {
                    attrIdx = i;
                    break;
                }
            }

            allRecords = manager.getDropAlterRecords(oldTable, attrIdx);
        } else if (method.equals("add")) {
            try {
                catalog.checkAddDuplicateAttr(tableName, attr.getName());
                checkDefaultValCorrectType(attr.getType().get(0), defaultVal);
            } catch (DuplicateAttributeNameException | AlterTableAddWrongDataTypeException e) {
                System.err.println(e);
                return false;
            }

            // adds old attrs and given attr to newAttrs
            for (Attribute oldAttr : oldAttrs) {
                newAttrs.add(oldAttr);
            }
            newAttrs.add(attr);

            allRecords = manager.getAddAlterRecords(oldTable, defaultVal);
        }

        // drops old table
        dropTableFunc(tableName);

        // add new table to the schema using newAttrs
        createTableFunc(tableName, newAttrs);

        // inserts all copied records into new table
        insertIntoTableFunc(tableName, allRecords);

        return true;
    }

    public boolean delete(DeleteQuery query) {
        String tableName = query.getName();
        WhereTree whereTree = query.getWhereTree();

        return  deleteFunc(tableName, whereTree);
    }

    /**
     *
     * @param tableName
     * @param whereTree
     * @return
     */
    public boolean deleteFunc(String tableName, WhereTree whereTree) {
        String tempTable = tableName;
        Table table = tables.get(tempTable);

        // gets index of primarykey
        // for delete with indexing
//        int pkIdx = -1;
//        ArrayList<Attribute> attributes = catalog.getSchemaGivenTableName(tableName);
//        for (int i = 0; i < attributes.size(); i++) {
//            Attribute attr = attributes.get(i);
//            if (attr.isPrimaryKey() == 1) {
//                pkIdx = i;
//                break;
//            }
//        }

        if (whereTree == null){
            ArrayList<Integer> pageIDs = table.getPages();
            for(int pageID : pageIDs) {
                Page page = manager.getBuffer().getPage(table, pageID);
                page.getContents().clear();
                page.setNumRecords(0);
            }
            return true;
        }

        ArrayList<ArrayList<Attribute>> columns = new ArrayList<>();

        columns.add(catalog.getSchemaGivenTableName(tableName));

        ArrayList<String> fromTables = new ArrayList<>();

        fromTables.add(tableName);

        constructTempSchema(columns, fromTables); // pass this into includeRow

        ArrayList<Integer> pages = table.getPages();
        Iterator<Integer> pgs = pages.iterator();
        while(pgs.hasNext()){
            Integer index = pgs.next();
            Page page = manager.getBuffer().getPage(table, index);

            ArrayList<Record> records = page.getContents();
            Iterator<Record> recs = records.iterator();
            while(recs.hasNext()){
                Record rec = recs.next();

                if (whereTree.includeRow(rec.getData(), constructTempSchema(columns, fromTables))){
                    recs.remove();
                    page.setNumRecords(page.getNumRecords() - 1);

                    // deletes from bplus tree if indexing is on does not work
                    // if(indexFlag){
                    // deleteBplus(tableName, recData.get(pkIndex);
                    // }
                }

                if(page.getNumRecords() == 0){
                    //pgs.remove();

                    //for(int j = index; j < table.getNumPages(); j++){
                    //updates pageIDs for remaining pages
                    //    pages.set(index, index);

                    //}
                }
            }
        }
        return true;

    }

    /**
     * updates column of table
     * @param query
     * @return
     */
    public boolean update(UpdateQuery query) {
        String tableName =  query.getName();
        String updateColumnName = query.getUpdateColumn();
        Object updateValue = query.getUpdateValue();
        WhereTree whereTree = query.getWhereTree();
//        Object pKey = null;

//        if(indexflag){
//            deleteBplus(tableName, pKey);
//            insertIntoBplus(tableName, new Record(updateValue));
//        }

        boolean success = true;

        try {
            catalog.checkTableExists(tableName, true);
        } catch (NoSuchTableException e) {
            System.err.println(e);
            return false;
        }

        // get column index
        int columnIdx = -1;
        ArrayList<Attribute> attributes = catalog.getSchemaGivenTableName(tableName);
        for (int i = 0; i < attributes.size(); i++) {
            Attribute attr = attributes.get(i);
            if (attr.getName().equals(updateColumnName)) {
                columnIdx = i;
                break;
            }
        }

        // getting column type
        int typeCheck = this.catalog.getTypeKey(attributes.get(columnIdx).getType().get(0));
        switch (typeCheck) {
            case (0): // int
                updateValue = Integer.parseInt(updateValue.toString());
                break;
            case (1): // double
                updateValue = Double.valueOf(updateValue.toString()).doubleValue();
                break;
            case (2): // bool
                updateValue = (Boolean)updateValue;
                break;
            case (3):
            case (4):
                updateValue = updateValue.toString();
        }

        // read and update records before deleting:
        Table table = tables.get(tableName);
        ArrayList<Record> allRecords = manager.getAllRecordsInTable(table);
        ArrayList<ArrayList<Object>> updatedRecords = new ArrayList<>();
        ArrayList<ArrayList<Object>> oldRecords = new ArrayList<>();

        ArrayList<ArrayList<Attribute>> columns = new ArrayList<>();
        columns.add(catalog.getSchemaGivenTableName(tableName));

        ArrayList<String> fromTables = new ArrayList<>();
        fromTables.add(tableName);

        ArrayList<ArrayList<String>> tempSchema = new ArrayList<>();
        tempSchema = constructTempSchema(columns, fromTables);

        // make list of updated records

        for (Record record: allRecords) {
            ArrayList<Object> recordData = record.getData();
            ArrayList<Object> oldValues = new ArrayList<>();
            for (Object value : recordData) {
                oldValues.add(value);
            }
            // adds record if it passes conditional
            if (whereTree.includeRow(recordData, tempSchema)) {
                ArrayList<Object> updatedRecord = new ArrayList<>();

                for (Object value : recordData) {
                    updatedRecord.add(value);
                }



                // update value in specified column
                updatedRecord.set(columnIdx, updateValue);

                // add new record to updated records
                updatedRecords.add(updatedRecord);
            }
            oldRecords.add(oldValues);
        }

        // delete specified records
        deleteFunc(tableName, whereTree);

        // insert and write all updated records
        boolean flag = updateRecordCheck(tableName, updatedRecords);
        if(!flag){
            deleteFunc(tableName, null);
            insertIntoTableFunc(tableName, oldRecords);
            System.err.println("Update aborted! Restoring database to instance before update call......");
            success = false;
        }

        return success;
    }

    /**
     * restores catalog and tables
     *
     * @param catalog
     */
    public void restoreDatabase(Catalog catalog) throws IOException {
        for (SchemaTable schema : catalog.getSchema().values()) {
            ArrayList<Integer> pOrder = schema.getPageOrder();
            int numPages = manager.buffer.readPageIDSSize(schema.getTableID());
            Table curTable = new Table(schema.getTableName(), schema.getTableID());
            for (int i = 0; i < numPages; i++) {
                Page p = manager.getBuffer().readPage(schema.getTableID(), i);
                p.setPageID(pOrder.get(i));
                // reconstruct a table and put in tables hashmap
                curTable.getPages().add(p.getPageID());
            }
            // curTable.getPages().addAll();
            this.tables.put(schema.getTableName(), curTable);
        }
    }

    public boolean insertIntoTable(InsertQuery query) {
        String tableName = query.getName();
        ArrayList<ArrayList<Object>> rows = query.getValues();

        return insertIntoTableFunc(tableName, rows);
    }

    private boolean updateRecordCheck(String tableName, ArrayList<ArrayList<Object>> rows) {
        boolean tableShouldExist = true;
        for (ArrayList<Object> row : rows) {
            try {
                catalog.checkTableExists(tableName, tableShouldExist);
                catalog.verifyInsertValue(tableName, row);
                checkDuplicatePrimaryKey(tableName, row);
                checkUniqueValues(tableName, row);
                checkNotNullValues(tableName, row);
                Table table = tables.get(tableName); // if we get here all values match schema
                Record record = new Record(row);
                manager.insertRecordIntoPage(table, record);

            } catch (NoSuchTableException | IncorrectAmountOfAttributesInRowException
                     | InvalidDataTypeInsertionException | CharLengthInsertValueIncorrectException
                     | VarcharLengthExceededException | DuplicatePrimaryKeyException
                     | InsertingNullIntoNotNullColumnException | InsertingDuplicateValueIntoUniqueColumnException e) {
                System.err.println(e);
                return false;
            }
        }
        return true;
    }

    //generates record pointer
    public ArrayList<Integer> generateRP(BPlusNode node, int index){
        //If new tree (only time inserting into empty node)
        if(node.getSearchKeys().size() == 0){
            ArrayList<Integer> newRP = new ArrayList<>();
            newRP.add(0);
            newRP.add(0);
            return newRP;
        }

//        if(index == -1){
//            index = node.getRecordPointers().size()-1;
//        }

        ArrayList<Integer> left = node.getRecordPointers().get(index -1);
        ArrayList<Integer> current = new ArrayList<>();
        current.add(left.get(0) + 1);
        current.add(left.get(1));

        return current;
    }

    // updates all indexes on the page if a record is inserted
    public void updateRecordPointers(ArrayList<Integer> rp, int pageNum, BPlusNode node, int startingIdx) {
        // checking if its the rightmost pointer in THIS node. If not, update every RP in this node that is to the right
        if(startingIdx != -1) {
            for(int i = startingIdx + 1; i < node.getRecordPointers().size(); i++) {
                // checking if still on the same page
                if(rp.get(1) == pageNum) {
                    int recordIdx = node.getRecordPointers().get(i).get(0) + 1; // increment index by one
                    node.getRecordPointers().get(i).set(0, recordIdx);// update index in record pointer
                }
            }
        }
        // recurse thorugh all other nodes to the right to increment index by one
        if(node.getNeighbor() != null) {
            updateRecordPointers(node.getNeighbor().getRecordPointers().get(0), pageNum, node.getNeighbor(), 0);
        }
    }

    public void deleteBplus(String tableName, Object primary){
        //use search for key and tree to try to find node I want to delete if it returns null there is no key
        //once you find it you have to delete - involves check size
        //search returns record pointer, index on page, page #
        //
        BPlusTree tree = trees.get(tableName);
        BPlusNode root = tree.getRoot();
        String pKeyType = tree.getPKeyType();
        ArrayList<Integer> recPoint = searchBplus(primary, tree, root, pKeyType, true, false);

        if(recPoint == null){
            //TODO: Pass in exception?
            //throw ;
        }
        else{
            //TODO: Check my thinking on this
            Table table = tables.get(tableName);
            //Get the page
            Page page = getBuffer().buffer.getPage(table, recPoint.get(1));
            //Removes the record
            page.getContents().remove(recPoint.get(0));
            page.setNumRecords(page.getNumRecords() - 1);

        }
    }

    /**
     * RETURNS NULL IF NOT FOUND
     *
     */
    public ArrayList<Integer> searchBplus(Object pKey,BPlusTree tree, BPlusNode node, String type, Boolean delete, Boolean repair){
        if(node.isLeaf()){
            for(Object key : node.getSearchKeys()) {
                switch (type) {
                    case ("integer"):
                        if (Integer.parseInt(pKey.toString()) == Integer.parseInt(key.toString())) {
                            ArrayList<Integer> rp = node.getRecordPointers().get(node.getSearchKeys().indexOf(key));
                            if(delete){
                                node.getRecordPointers().remove(rp);
                                node.getSearchKeys().remove(key);
                                //check for merge and borrow
                                if(isUnderflow(node, tree.getN())){
                                    manageDeletion(tree,node,type);
                                }

                            }else if (repair){
                                // this will update the first record of the new page
                                //
                                node.getRecordPointers().set(node.getSearchKeys().indexOf(key), rp);
                                for(int i = node.getSearchKeys().indexOf(key) + 1; i < node.getRecordPointers().size(); i++) {
                                    if(rp.get(1) == node.getRecordPointers().get(i).get(1)) {
                                        int recordIdx = node.getRecordPointers().get(i).get(0) + 1; // increment index by one
                                        node.getRecordPointers().get(i).set(0, recordIdx);// update index in record pointer
                                    }
                                }

                            }
                            return rp;
                        }
                        break;
                    case ("double"): // double
                        if (Double.valueOf(pKey.toString()).doubleValue() == Double.valueOf(key.toString()).doubleValue()) {
                            ArrayList<Integer> rp = node.getRecordPointers().get(node.getSearchKeys().indexOf(key));
                            if(delete){
                                node.getRecordPointers().remove(rp);
                                node.getSearchKeys().remove(key);

                                //check for merge and borrow
                            }else if (repair){
                                // this will update the first record of the new page
                                //
                                node.getRecordPointers().set(node.getSearchKeys().indexOf(key), rp);
                                for(int i = node.getSearchKeys().indexOf(key) + 1; i < node.getRecordPointers().size(); i++) {
                                    if(rp.get(1) == node.getRecordPointers().get(i).get(1)) {
                                        int recordIdx = node.getRecordPointers().get(i).get(0) + 1; // increment index by one
                                        node.getRecordPointers().get(i).set(0, recordIdx);// update index in record pointer
                                    }
                                }

                            }
                            return rp;
                        }
                        break;
                    case ("char"):
                    case ("varchar"):
                        if (pKey.toString().equals(key.toString())) {
                            ArrayList<Integer> rp =node.getRecordPointers().get(node.getSearchKeys().indexOf(key));
                            if(delete){
                                node.getRecordPointers().remove(rp);
                                node.getSearchKeys().remove(key);

                            }else if (repair){
                                // this will update the first record of the new page
                                //
                                node.getRecordPointers().set(node.getSearchKeys().indexOf(key), rp);
                                for(int i = node.getSearchKeys().indexOf(key) + 1; i < node.getRecordPointers().size(); i++) {
                                    if(rp.get(1) == node.getRecordPointers().get(i).get(1)) {
                                        int recordIdx = node.getRecordPointers().get(i).get(0) + 1; // increment index by one
                                        node.getRecordPointers().get(i).set(0, recordIdx);// update index in record pointer
                                    }
                                }

                            }
                            return rp;
                        }
                        break;
                }
            }
            return null;
        }else {
            // child index to traverse
            int findIndex = 0;
            // going through the node and taking the "pointer" before the searchKey if less than
            // if greater than all search keys, go to last child
            for(Object key : node.getSearchKeys()) {
                // using switch to evaluate different types of primary keys
                switch (type) {
                    case ("integer"):
                        // take pointer to child at same index as this search key
                        // all other are the same with different type casts
                        if(Integer.parseInt(pKey.toString()) < Integer.parseInt(key.toString())) {
                            searchBplus(pKey, tree,  node.getChildren().get(node.getSearchKeys().indexOf(key)), type, delete, repair);
                            // set to negative 1 to break loop later. Dont want to loop afet traversing already
                            findIndex = -1;
                        }
                        break;
                    case ("double"): // double
                        if(Double.valueOf(pKey.toString()).doubleValue() < Double.valueOf(key.toString()).doubleValue()) {
                            searchBplus(pKey, tree,  node.getChildren().get(node.getSearchKeys().indexOf(key)), type, delete, repair);
                            // set to negative 1 to break loop later. Dont want to loop afet traversing already
                            findIndex = -1;
                        }
                        break;
                    case ("char"):
                    case ("varchar"):
                        if(pKey.toString().compareTo(key.toString()) < 0) {
                            searchBplus(pKey, tree,  node.getChildren().get(node.getSearchKeys().indexOf(key)), type, delete, repair);
                            // set to negative 1 to break loop later. Dont want to loop afet traversing already
                            findIndex = -1;
                        }
                        break;
                }
                // go to next child or break if done already
                if(findIndex == -1) {
                    break;
                } else {
                    return searchBplus(pKey, tree,  node.getChildren().get(node.getChildren().size() - 1), type, delete, repair);
                }
            }
        }



        return null;
    }


    private void manageDeletion(BPlusTree tree, BPlusNode node, String type){
        // determine its a merge or borrow
        // if merging would cause an overflow we dont merge
        // what is supposed to happen internal node looks to merge with neighbors
        // if not possible it pulls down a search key from parent node to merge with neighbor.

        boolean isLeftChild = true;
        BPlusNode rightNeighbor = node.getNeighbor();

        int leftNodeIdx = node.getParent().getChildren().indexOf(node)  - 1 ;
        if(leftNodeIdx < 0){
            isLeftChild = false;
        }
        BPlusNode leftNeighbor = node.getParent().getChildren().get(leftNodeIdx);

        // TODO EDGE CASE for roots
        if((node.getSearchKeys().size() + leftNeighbor.getSearchKeys().size()) < tree.getN() - 1 && isLeftChild){
            if(node.isLeaf()){
                leftNeighbor.getRecordPointers().addAll(node.getRecordPointers());
            } else if (node.isInternal() && !node.isRoot()) {
                leftNeighbor.getChildren().addAll(node.getChildren());
                for(BPlusNode child : leftNeighbor.getChildren()){
                    child.setParent(leftNeighbor);
                }
            }
            leftNeighbor.getSearchKeys().addAll(node.getSearchKeys());
            node.getSearchKeys().remove(leftNodeIdx);
            node.getParent().getChildren().remove(node);
            leftNeighbor.setNeighbor(node.getNeighbor()); // doesnt matter for not leaf all null



        } else if ((node.getSearchKeys().size() + rightNeighbor.getSearchKeys().size()) < tree.getN() - 1) {
            if(node.isLeaf()){
                node.getRecordPointers().addAll(rightNeighbor.getRecordPointers());
            } else if (node.isInternal() && !node.isRoot()) {
                rightNeighbor = node.getParent().getChildren().get( node.getParent().getChildren().indexOf(node) + 1);
                node.getChildren().addAll(rightNeighbor.getChildren());
                for(BPlusNode child : node.getChildren()){
                    child.setParent(node);
                }
            }
            ArrayList<Object> rightChildSearchKeys = rightNeighbor.getSearchKeys();
            Object smallestSearchKey = rightChildSearchKeys.get(0);

            node.getSearchKeys().addAll(rightNeighbor.getSearchKeys());
            node.getSearchKeys().remove(smallestSearchKey);
            node.getParent().getChildren().remove(rightNeighbor);
            node.setNeighbor(rightNeighbor.getNeighbor());

        }else { // borrowing because merge did not work

            ArrayList<Object> leftChildSearchKeys = leftNeighbor.getSearchKeys();
            ArrayList<ArrayList<Integer>> leftChildRecordPointers = leftNeighbor.getRecordPointers();
            Object biggestSearchKey = leftChildSearchKeys.get(leftChildSearchKeys.size() -1);
            ArrayList<Integer> biggestRecordPointer = leftChildRecordPointers.get(leftChildRecordPointers.size() -1);
            int idxOfbiggestSearchKeyAndRecordPointer = leftNeighbor.getSearchKeys().indexOf(biggestSearchKey);

            leftNeighbor.getSearchKeys().remove(idxOfbiggestSearchKeyAndRecordPointer);
            leftNeighbor.getRecordPointers().remove(idxOfbiggestSearchKeyAndRecordPointer);
            node.addRP(biggestRecordPointer, 0);
            node.addSearchKey(biggestSearchKey, 0);
            node.getParent().getSearchKeys().set(leftNodeIdx, biggestSearchKey);
        }
        if(isUnderflow(node.getParent(), tree.getN())){ // recursion
            manageDeletion(tree, node.getParent(), type);
        }
    }


    private boolean isUnderflow(BPlusNode node, int n){
        if(node.isRoot()){
            return node.getSearchKeys().size() == 0;
        } else if (node.isInternal()) {
            double minP = (double)n/2;
            return node.getSearchKeys().size() < (int)Math.ceil(minP);
        }
        // node.isLeaf()
        double minP = (double)(n-1)/2;
        return node.getSearchKeys().size() < (int)Math.ceil(minP);
    }


    public ArrayList<Integer> insertIntoBplus(String tableName, Record record) {
        BPlusTree tree = trees.get(tableName);
        int pKeyIdx = tree.getPKeyIdx(); //getting primary key of record
        String pKeyType = tree.getPKeyType(); // getting primary key type for casting
        // getting primary key from record
        Object pKey = record.getData().get(pKeyIdx);
        // for testing use n = 3
        return traverseTreeInsert(tree, tree.getRoot(), pKey, pKeyType, null, 3);
        //return traverseTreeInsert(tree, tree.getRoot(), pKey, pKeyType, null, tree.getN());
    }

    public ArrayList<Integer> traverseTreeInsert(BPlusTree tree, BPlusNode node, Object pKey, String type, BPlusNode previous, int n) {
        // Record pointer object to be inserted
        ArrayList<Integer> recordPointer = new ArrayList<>();

        // checking of in an insertable node
        if (node.isLeaf()) {
            // If node is empty, just add it. Typically  when you first make a tree
            if(node.getNumOfRecPointers() == 0) {
                recordPointer = generateRP(node, -1);
                node.addRP(recordPointer, -1);
                node.addSearchKey(pKey, -1);
                updateRecordPointers(recordPointer, recordPointer.get(1), node, -1);
                return recordPointer;
            } else {
                // going through the node and inserting at proper spot
                boolean isInserted = false;
                for(Object key : node.getSearchKeys()) {
                    // using switch to evaluate different types of primary keys
                    switch (type) {
                        case ("integer"):
                            // insert at current index at first occurance of less than
                            // all other are the same with different type casts
                            if(Integer.parseInt(pKey.toString()) < Integer.parseInt(key.toString())) {
                                int index = node.getSearchKeys().indexOf(key); // argument of addRP
                                recordPointer = generateRP(node, index);
                                node.addRP(recordPointer, index);
                                node.addSearchKey(pKey, index);
                                updateRecordPointers(recordPointer, recordPointer.get(1), node, index);
                                isInserted = true;
                            }
                            break;
                        case ("double"): // double
                            if(Double.valueOf(pKey.toString()).doubleValue() < Double.valueOf(key.toString()).doubleValue()) {
                                int index = node.getSearchKeys().indexOf(key); // argument of addRP
                                recordPointer = generateRP(node, index);
                                node.addRP(recordPointer, index);
                                node.addSearchKey(pKey, index);
                                updateRecordPointers(recordPointer, recordPointer.get(1), node, index + 1);
                                isInserted = true;
                            }

                            break;
                        case ("char"):
                        case ("varchar"):
                            if(pKey.toString().compareTo(key.toString()) < 0) {
                                int index = node.getSearchKeys().indexOf(key); // argument of addRP
                                recordPointer = generateRP(node, index);
                                node.addRP(recordPointer, index);
                                node.addSearchKey(pKey, index);
                                updateRecordPointers(recordPointer, recordPointer.get(1), node, index + 1);
                                isInserted = true;
                            }
                            break;
                    }
                    if(isInserted){
                        break;
                    }
                }
                // if not less than anything, add to end
                if(!isInserted){
                    recordPointer = generateRP(node, node.getRecordPointers().size());
                    node.addRP(recordPointer, -1);
                    node.addSearchKey(pKey, -1);
                    updateRecordPointers(recordPointer, recordPointer.get(1), node, -1);
                }
            }
            // check if node is overfull, if so split it
            if(node.getSearchKeys().size() > n-1) {
                // splits a leaf node

                int halfIdx = node.getSearchKeys().size()/2; // index to split on
                ArrayList<Object> leftSearchKeys = new ArrayList<>(node.getSearchKeys().subList(0, halfIdx));
                ArrayList<Object> rightSearchKeys = new ArrayList<>(node.getSearchKeys().subList(halfIdx, node.getSearchKeys().size()));

                ArrayList<ArrayList<Integer>> leftRecordPointers = new ArrayList<>(node.getRecordPointers().subList(0, halfIdx));
                ArrayList<ArrayList<Integer>> rightRecordPointers = new ArrayList<>(node.getRecordPointers().subList(halfIdx, node.getSearchKeys().size()));
                if(node.isRoot() && node.isLeaf()){
                    Object smallestSearchKeyNewRoot = rightSearchKeys.get(0);
                    BPlusNode newRoot = new BPlusNode(true, false, true);
                    newRoot.getSearchKeys().add(smallestSearchKeyNewRoot);
                    tree.setRoot(newRoot);

                    BPlusNode left = new BPlusNode(false, true, false);
                    left.setRecordPointers(leftRecordPointers);
                    left.setSearchKeys(leftSearchKeys);
                    left.setNumOfRecPointers(leftRecordPointers.size());
                    left.setParent(tree.getRoot());

                    BPlusNode right = new BPlusNode(false, true, false);
                    right.setRecordPointers(rightRecordPointers);
                    right.setSearchKeys(rightSearchKeys);
                    right.setNumOfRecPointers(rightRecordPointers.size());
                    right.setParent(tree.getRoot());

                    left.setNeighbor(right);

                    tree.getRoot().getChildren().add(left);

                    tree.getRoot().getChildren().add(right);

                    return recordPointer;
                } else { // normal split
                    splitNode(tree, node, type, n, leftSearchKeys, rightSearchKeys, leftRecordPointers, rightRecordPointers);
                    return recordPointer;
                }

            }
        }
        else {
            // FOR SEARCHING FOR THE RIGHT NODE
            // child index to traverse
            int findIndex = 0;
            // going through the node and taking the "pointer" before the searchKey if less than
            // if greater than all search keys, go to last child
            for(Object key : node.getSearchKeys()) {
                // using switch to evaluate different types of primary keys
                switch (type) {
                    case ("integer"):
                        // take pointer to child at same index as this search key
                        // all other are the same with different type casts
                        if(Integer.parseInt(pKey.toString()) < Integer.parseInt(key.toString())) {
                            return traverseTreeInsert(tree, node.getChildren().get(node.getSearchKeys().indexOf(key)), pKey, type, node, n);
                            // set to negative 1 to break loop later. Dont want to loop afet traversing already
                            //findIndex = -1;
                        }
                        break;
                    case ("double"): // double
                        if(Double.valueOf(pKey.toString()).doubleValue() < Double.valueOf(key.toString()).doubleValue()) {
                            return traverseTreeInsert(tree, node.getChildren().get(node.getSearchKeys().indexOf(key)), pKey, type, node, n);
                            // set to negative 1 to break loop later. Dont want to loop afet traversing already
                            //findIndex = -1;
                        }
                        break;
                    case ("char"):
                    case ("varchar"):
                        if(pKey.toString().compareTo(key.toString()) < 0) {
                            return traverseTreeInsert(tree, node.getChildren().get(node.getSearchKeys().indexOf(key)), pKey, type, node, n);
                            // set to negative 1 to break loop later. Dont want to loop afet traversing already
                            //findIndex = -1;
                        }
                        break;
                }
                // go to next child or break if done already
                if(findIndex == -1) {
                    break;
                } else {
                    return traverseTreeInsert(tree, node.getChildren().get(node.getSearchKeys().size()), pKey, type, node, n);
                }
            }

        }

        return recordPointer;
    }

    private void splitNode(BPlusTree tree, BPlusNode node, String type, int n,
                           ArrayList<Object> leftSearchKeys, ArrayList<Object> rightSearchKeys,
                           ArrayList<ArrayList<Integer>> leftRecordPointers, ArrayList<ArrayList<Integer>> rightRecordPointers) {

        // BASE CASE HAPPENS ONLY IF THE PARENT HAS TO SPLIT
        if(node.isRoot() && node.isInternal()){
            Object smallestSearchKeyNewRoot = rightSearchKeys.get(0);
            BPlusNode newRoot = new BPlusNode(true, false, true);
            newRoot.getSearchKeys().add(smallestSearchKeyNewRoot);
            tree.setRoot(newRoot);

            BPlusNode left = new BPlusNode(false, false, true);
            left.setSearchKeys(leftSearchKeys);
            left.setParent(tree.getRoot());

            ArrayList<BPlusNode> leftChildren = new ArrayList<>(node.getChildren().subList(0, left.getSearchKeys().size()+1));
            left.getChildren().addAll(leftChildren);
            for(BPlusNode nde : left.getChildren()){
                nde.setParent(left);
            }

            BPlusNode right = new BPlusNode(false, false, true);
            rightSearchKeys.remove(smallestSearchKeyNewRoot);
            right.setSearchKeys(rightSearchKeys);
            right.setParent(tree.getRoot());

            ArrayList<BPlusNode> rightChildren = new ArrayList<>(node.getChildren().subList(left.getSearchKeys().size()+1, node.getChildren().size()));
            right.getChildren().addAll(rightChildren);
            for(BPlusNode nde : right.getChildren()){
                nde.setParent(right);
            }


            tree.getRoot().getChildren().add(left);

            tree.getRoot().getChildren().add(right);
            // we are done no more recursion
        } else {
            Object smallestSearchKey = rightSearchKeys.get(0);

            // Here we need to insert the search key. so we need to check where it goes by comparing values
            boolean isAdded = false;
            for(Object key : node.getParent().getSearchKeys()) {
                switch (type) {
                    case ("integer"):
                        if(Integer.parseInt(smallestSearchKey.toString()) < Integer.parseInt(key.toString())) {
                            int index = node.getSearchKeys().indexOf(key);
                            node.getParent().addSearchKey(smallestSearchKey, index);
                            isAdded = true;
                        }
                        break;
                    case ("double"): // double
                        if(Double.valueOf(smallestSearchKey.toString()).doubleValue() < Double.valueOf(key.toString()).doubleValue()) {
                            int index = node.getSearchKeys().indexOf(key);
                            node.getParent().addSearchKey(smallestSearchKey, index);
                            isAdded = true;
                        }

                        break;
                    case ("char"):
                    case ("varchar"):
                        if(smallestSearchKey.toString().compareTo(key.toString()) < 0) {
                            int index = node.getSearchKeys().indexOf(key);
                            node.getParent().addSearchKey(smallestSearchKey, index);
                            isAdded = true;
                        }
                        break;
                }
            }
            if(!isAdded){
                node.getParent().addSearchKey(smallestSearchKey, -1);
            }

            BPlusNode oldNeighbor = node.getNeighbor();
            BPlusNode left = node;
            left.setSearchKeys(leftSearchKeys);
            if(left.isLeaf()){
                left.setRecordPointers(leftRecordPointers); // note is null if it is not a leaf node.
                left.setNumOfRecPointers(leftRecordPointers.size());
            }


            BPlusNode right = new BPlusNode(false, node.isLeaf(), node.isInternal());
            if(right.isLeaf()){
                right.setRecordPointers(rightRecordPointers); // note is null if it is not a leaf node.
                right.setNumOfRecPointers(rightRecordPointers.size());
            }
            right.setSearchKeys(rightSearchKeys);
            right.setParent(node.getParent());
            if(right.getParent().isRoot() && right.isInternal()){
                right.getSearchKeys().remove(0);
            }


            if(left.isInternal() && right.isInternal()){
                ArrayList<BPlusNode> leftChildren = new ArrayList<>(node.getChildren().subList(0, left.getSearchKeys().size()+1));
                ArrayList<BPlusNode> rightChildren = new ArrayList<>(node.getChildren().subList(left.getSearchKeys().size()+1, node.getChildren().size()));
                left.setChildren(leftChildren);
                for(BPlusNode nde : left.getChildren()){
                    nde.setParent(left);
                }

                // TODO LEFT CHILDREN ERASED
                right.getChildren().addAll(rightChildren);
                for(BPlusNode nde : right.getChildren()){
                    nde.setParent(right);
                }
            }
            // index that left child will go to. +1 for right child
            int childIdx = node.getParent().getSearchKeys().indexOf(smallestSearchKey);
            node.getParent().getChildren().add(childIdx + 1, right);


            if(node.isLeaf()){
                left.setNeighbor(right);
                right.setNeighbor(oldNeighbor);
            }
            if (node.getParent().getSearchKeys().size() >= n){ // TODO might not be >= might be >

                int halfIdx = node.getParent().getSearchKeys().size()/2; // index to split on
                ArrayList<Object> parentLeftSearchKeys = new ArrayList<>(node.getParent().getSearchKeys().subList(0, halfIdx));
                ArrayList<Object> parentRightSearchKeys = new ArrayList<>(node.getParent().getSearchKeys().subList(halfIdx, node.getParent().getSearchKeys().size()));

                splitNode(tree, node.getParent(), type, n, parentLeftSearchKeys, parentRightSearchKeys, leftRecordPointers, rightRecordPointers);
            }
        }
    }


    private boolean insertIntoTableFunc(String tableName, ArrayList<ArrayList<Object>> rows) {
        boolean tableShouldExist = true;
        boolean passedAllInserts = true;
        for (ArrayList<Object> row : rows) {
            try {
                catalog.checkTableExists(tableName, tableShouldExist);
                catalog.verifyInsertValue(tableName, row);
                checkDuplicatePrimaryKey(tableName, row);
                checkUniqueValues(tableName, row);
                checkNotNullValues(tableName, row);
                Table table = tables.get(tableName); // if we get here all values match schema
                Record record = new Record(row);
                if(indexflag) {
                    ArrayList<Integer> rp = insertIntoBplus(tableName, record);
                    manager.insertUsingRP(rp, record, table);
                }
                else{
                    // idx of the record that was just inserted into the page
                    manager.insertRecordIntoPage(table, record);
                }

            } catch (NoSuchTableException | IncorrectAmountOfAttributesInRowException
                     | InvalidDataTypeInsertionException | CharLengthInsertValueIncorrectException
                     | VarcharLengthExceededException | DuplicatePrimaryKeyException
                     | InsertingNullIntoNotNullColumnException | InsertingDuplicateValueIntoUniqueColumnException e) {
                System.err.println(e);
                passedAllInserts = false;
                break;
            }
        }
        return passedAllInserts;
    }


    public void checkDefaultValCorrectType(String typeStr, Object defaultVal) {
        boolean correctType = true;
        if (defaultVal == null) {
            return;
        }
        switch (typeStr) {
            case "integer":
                if (!(defaultVal instanceof Integer)) {
                    correctType = false;
                }
                break;
            case "double":
                if (!(defaultVal instanceof Double)) {
                    correctType = false;
                }
                break;
            case "boolean":
                if (!(defaultVal instanceof Boolean)) {
                    correctType = false;
                }
                break;
            case "char":
            case "varchar":
                if (!(defaultVal instanceof String)) {
                    correctType = false;
                }
                break;
        }

        if (!correctType) {
            throw new AlterTableAddWrongDataTypeException(
                    String.format("GIVEN TYPE OF ATTRIBUTE AND TYPE OF DEFAULT VALUE ARE INCONSISTENT"));
        }
    }

    public void checkDuplicatePrimaryKey(String name, ArrayList<Object> row) {
        ArrayList<Attribute> schema = catalog.getSchema().get(name).getAttributes();
        for (int i = 0; i < row.size(); i++) { // foreach value in the row
            Table t = tables.get(name); // table guaranteed to exist check done in parent method
            ArrayList<Integer> pageIDs = t.getPages();
            for (int pageNum = 0; pageNum < pageIDs.size(); pageNum++) {
                Page page = manager.getBuffer().getPage(t, pageNum);
                for (Record record : page.getContents()) {
                    ArrayList<Object> data = record.getData();
                    for (int j = 0; j < data.size(); j++) {
                        if (schema.get(j).isPrimaryKey() == 1) {
                            if (row.get(j).equals(data.get(j))) {
                                StringBuilder sb = new StringBuilder();
                                for (int k = 0; k < row.size(); k++) {
                                    if (row.get(k) == null) {
                                        sb.append("null");
                                    } else {
                                        sb.append(row.get(k).toString());
                                    }
                                    if (!(k == row.size() - 1)) {
                                        sb.append(" ");
                                    }
                                }
                                throw new DuplicatePrimaryKeyException(
                                        String.format("DUPLICATE PRIMARY KEY: row(%s)", sb));

                            }
                        }
                    }
                }
            }
        }
    }

    public void checkNotNullValues(String name, ArrayList<Object> row) {
        ArrayList<Attribute> schema = catalog.getSchema().get(name).getAttributes();
        for (int i = 0; i < row.size(); i++) { // foreach value in the row
            if (schema.get(i).isNotNull()) {
                if (row.get(i) == null) {
                    throw new InsertingNullIntoNotNullColumnException(
                            String.format("NULL VALUE IN NOT NULL COLUMN: column(%s: %s)", schema.get(i).getName(),
                                    schema.get(i).typeString()));
                }
            }
        }
    }

    public void checkUniqueValues(String name, ArrayList<Object> row) {
        ArrayList<Attribute> schema = catalog.getSchema().get(name).getAttributes();
        for (int i = 0; i < row.size(); i++) { // foreach value in the row
            Table t = tables.get(name); // table guaranteed to exist check done in parent method
            ArrayList<Integer> pageIDs = t.getPages();
            for (int pageNum = 0; pageNum < pageIDs.size(); pageNum++) {
                Page page = manager.getBuffer().getPage(t, pageNum);
                for (Record record : page.getContents()) {
                    ArrayList<Object> data = record.getData();
                    for (int j = 0; j < data.size(); j++) {
                        if (schema.get(j).isUnique()) {
                            if (row.get(j).equals(data.get(j))) { // cannot happen
                                StringBuilder sb = new StringBuilder();
                                for (int k = 0; k < row.size(); k++) {
                                    if (row.get(k) == null) {
                                        sb.append("null");
                                    } else {
                                        sb.append(row.get(k).toString());
                                    }
                                    if (!(k == row.size() - 1)) {
                                        sb.append(" ");
                                    }
                                }
                                throw new InsertingDuplicateValueIntoUniqueColumnException(
                                        String.format("DUPLICATE VALUES IN UNIQUE COLUMN: value: %s in column(%s: %s)",
                                                row.get(j).toString(), schema.get(j).getName(),
                                                schema.get(j).typeString()));
                            }
                        }
                    }
                }
            }
        }
    }

    // helper function to reduce redundant code
    public ArrayList<Record> loadTable(ArrayList<Record> megaTable, int startingTable, int[] numPages, ArrayList<Table> selectedTables, List<List<Integer>> pageIds) {
        int curNumPage = numPages[startingTable];
        for (int j = 0; j < curNumPage; j++) {
            Table table = selectedTables.get(startingTable);
            int pageID = pageIds.get(startingTable).get(j);
            Page page = manager.getBuffer().getPage( table, pageID );
            ArrayList<Record> pageContents = page.getContents();
            megaTable.addAll( pageContents );
        }

        return megaTable;
    }

    // constructs the whole "schema" for cartesian product result set(megaTable)
    public ArrayList<ArrayList<String>> constructTempSchema(ArrayList<ArrayList<Attribute>> columns, ArrayList<String> fromTables) {
        ArrayList<ArrayList<String>> tempSchema = new ArrayList<>();
        for(int i = 0; i < columns.size(); i++) {
            for(int j = 0; j < columns.get(i).size(); j++) {
                ArrayList<String> curTable = new ArrayList<>();
                curTable.add(fromTables.get(i));
                curTable.add(columns.get(i).get(j).getName());
                tempSchema.add(curTable);
            }
        }
        return tempSchema;
    }

    // creates a dummy record that holds all the data of 2 records. BITMAP NOT UPDATED
    public Record mergeRecord(Record left, Record right) {
        ArrayList<Object> merged = new ArrayList<>();
        merged.addAll(left.getData());
        merged.addAll(right.getData());
        return new Record(merged);
    }

    // creates table of cartesian product on 2 tables
    public ArrayList<Record> cartesianProduct(ArrayList<Record> megaTable, ArrayList<Record> temp) {
        ArrayList<Record> newMegaTable = new ArrayList<>();
        for(int i = 0; i < megaTable.size(); i++) {
            for(int j = 0; j < temp.size(); j++) {
                newMegaTable.add(mergeRecord(megaTable.get(i), temp.get(j)));
            }
        }
        return newMegaTable;
    }

    /**
     * Finds the table, col pos that the cartisean result set table will be ordered by. By retrieving the table and col
     *      info from the SelectQuery, it will then iterate through the megaschema until it finds a match. It then
     *      returns this index value so when insertion sort is performed on the cartisean result set array of valid
     *      records, it knows what index to sort on.
     * @param query SelectQuery retrieves the table and col to be ordered through .getOrderTableAndColumn getter
     * @param megaSchema Schema for the cartisean result set (joined table)
     * @return Index integer position on the megaschema.
     */
    private int findOrderByIndexOnMegaSchema(SelectQuery query, ArrayList<ArrayList<String>> megaSchema){
        //index on valid where to sort
        int index = -1;
        //table name to order by
        String tableName = query.getOrderbyTableAndColumn().get(0);
        //col name to order by
        String colName = query.getOrderbyTableAndColumn().get(1);
        //need to fetch table position
        for(int i = 0; i < megaSchema.size(); i++) {
            String curTableName = megaSchema.get(i).get(0);
            //Check to see if tableName is null in query parse string. If it's null, then just check match for col name
            if(tableName == null){
                String curColName = megaSchema.get(i).get(1);
                if(colName.equals(curColName)){
                    index = i;
                    break;
                }
            }

            //tableName exists, check for index looking at both tableName and colName match from query parse
            else {
                if (tableName.equals(curTableName)) {
                    String curColName = megaSchema.get(i).get(1);
                    if (colName.equals(curColName)) {
                        index = i;
                        break;
                    }
                }
            }
        }

        return index;
    }

    /**
     * Performs insertion sort on selected records before printing
     *
     * @param records Arraylist of records selected
     * @param index The index in the record tuple that is being sorted
     */
    private static void insertionSortRecords(ArrayList<Record> records, int index){
        for(int r = 0; r < records.size(); r++){
            Record current = records.get(r);
            int iterator = r - 1;
            /* Note: Don't get bogged down by long compare portion of code. It's just long since I need to access
                    a records array list of objects in order to sort. So I need to us records getData() to get list of
                    objects and then retrieve the object that is being sorted using the index value calculated before.
                    If other portion of code doesn't make sense, look up insertion sort algo
             */
            while( (iterator > -1) && records.get(iterator).compare(current.getData().get(index), records.get(iterator).getData().get(index)) > 0){
                records.set(iterator+1, records.get(iterator));
                iterator--;
            }
            records.set(iterator+1, current);
        }
    }

    // Select SQL statement. Supports *, FROM, WHERE, and ORDERBY
    public boolean select(SelectQuery query) {
        boolean tableShouldExist = true;
        int cartesian = 0; // tells me if cartesian product has been performed
        try {
            // get FROM name
            ArrayList<String> fromTables = query.getFromTables();
            for (String name : fromTables) {
                catalog.checkTableExists(name, tableShouldExist);
            }

            ArrayList<ArrayList<Attribute>> columns = new ArrayList<>();
            ArrayList<Table> selectedTables = new ArrayList<>();
            for (String name : fromTables) {
                columns.add(catalog.getSchemaGivenTableName(name)); // get all schemas for FROM tables
                selectedTables.add(tables.get(name)); // getting all Table objects
            }
            int[] numPages = new int[selectedTables.size()];
            List<List<Integer>> pageIds = new ArrayList<>();
            for(int i = 0; i < selectedTables.size(); i++) {
                numPages[i] = selectedTables.get(i).getNumPages(); // get number of pages for specific table
                pageIds.add(selectedTables.get(i).getPages()); // list of pageIds
            }

            // first table
            ArrayList<Record> megaTable = new ArrayList<>();

            // loading first table into Java. This will always happen
            loadTable( megaTable, 0, numPages, selectedTables, pageIds);
            // "schema" of cartesian product
            ArrayList<ArrayList<String>> megaSchema = new ArrayList<>();
            megaSchema = constructTempSchema(columns, fromTables);
            if(selectedTables.size() > 1) { // need to cartesian product
                cartesian = 1;
                for( int i = 1; i < selectedTables.size(); i++) {
                    ArrayList<Record> temp = new ArrayList<>();
                    // load next table into java
                    loadTable( temp, i, numPages, selectedTables, pageIds);
                    // preforming cartesian product
                    megaTable = cartesianProduct(megaTable, temp);
                }
            }
            // checking if row follows WHERE restrictions
            ArrayList<Record> validRecords = new ArrayList<>();
            if(query.getWhereTree() == null) { // if no WHERE, use all records
                validRecords = megaTable;
            } else { // if WHERE exists, only take the valid ones
                for (Record r : megaTable) {
                    if(query.getWhereTree().includeRow(r.getData(), megaSchema)) {
                        validRecords.add(r);
                    }
                }
            }

            // Finding index of SELECTed attributes so we know which ones to print
            ArrayList<ArrayList<String>> selectedAttr = query.getSelectTablesAndColumns();
            ArrayList<Integer> selectedColIdx = new ArrayList<>();
            if(selectedAttr != null) {
                if(cartesian == 0) { // if no cartesian table names will be null
                    for(ArrayList<String> str : selectedAttr) {
                        for(int i = 0; i < megaSchema.size(); i++) {
                            if(megaSchema.get(i).contains(str.get(1))) {
                                selectedColIdx.add(i);
                            }
                        }
                    }
                } else {
                    for(ArrayList<String> str : selectedAttr) {
                        if(megaSchema.contains(str)) {
                            selectedColIdx.add(megaSchema.indexOf(str));
                        }
                    }
                }
            } else {
                // if select * set indexes to select all columns
                for(int i = 0; i < megaSchema.size(); i++) {
                    selectedColIdx.add(i);
                }
            }

            // initializing max column length based on atribute name
            int numCol = selectedColIdx.size();
            int[] maxColLen = new int[numCol];
            for (int i = 0; i < numCol; i++) {
                maxColLen[i] = megaSchema.get(selectedColIdx.get(i)).get(1).length();
            }
            // adjusting max size of each column based on char len of entries
            for(Record record : validRecords) {
                for (int i = 0; i < numCol; i++) {
                    int strLen = String.valueOf(record.getData().get(selectedColIdx.get(i))).length();
                    if (maxColLen[i] < strLen) {
                        maxColLen[i] = strLen;
                    }
                }
            }


            // setting up the header
            String header = "";
            int headerSum = 1;
            Object[] headerArgs = new Object[numCol];
            for (int i = 0; i < numCol; i++) {
                int tempLen = maxColLen[i];
                headerSum += tempLen + 3;
                header += "| %-" + String.valueOf(tempLen) + "s ";
                headerArgs[i] = megaSchema.get(selectedColIdx.get(i)).get(1);
            }
            header += "|\n";
            String horizantal = new String(new char[headerSum]).replace('\0', '-');
            // output header
            System.out.println(horizantal);
            System.out.printf(header, headerArgs);
            System.out.println(horizantal);

            /** ORDERBY portion */

            //Order record before print or not
            if(query.getOrderbyTableAndColumn() != null){
                int index = findOrderByIndexOnMegaSchema(query, megaSchema);
                insertionSortRecords(validRecords, index);
            }

            // format records and print them 1 by 1
            for (int i = 0; i < validRecords.size(); i++) {
                String row = "";
                Object[] rowArgs = new Object[numCol];
                for (int j = 0; j < numCol; j++) {
                    row += "|%" + String.valueOf(maxColLen[j] + 2) + "s";
                    if (validRecords.get(i).getData().get(selectedColIdx.get(j)) == null) {
                        rowArgs[j] = "null";
                    } else {
                        rowArgs[j] = validRecords.get(i).getData().get(selectedColIdx.get(j)).toString();
                    }

                }
                row += "|\n";
                System.out.printf(row, rowArgs);
            }
        } catch (NoSuchTableException e) {
            System.err.println(e);
            return false;
        }
        return true;
    }

    public boolean displayInfo(String name, boolean printIndented) {
        boolean tableShouldExist = true;
        try {
            catalog.checkTableExists(name, tableShouldExist);
            if (printIndented) {
                System.out.print("\t");
            }
            System.out.println(String.format("Table name: %s", name));
            if (printIndented) {
                System.out.print("\t");
            }
            System.out.println("Table schema:");
            ArrayList<Attribute> schema = catalog.getSchemaGivenTableName(name);
            for (Attribute attribute : schema) {
                if (attribute.getType().get(0).equals("char") || attribute.getType().get(0).equals("varchar")) {
                    if (printIndented) {
                        System.out.print("\t");
                    }
                    System.out.print(String.format("\t%s: %s(%s)", attribute.getName(),
                            attribute.getType().get(0), attribute.getType().get(1)));
                } else {
                    if (printIndented) {
                        System.out.print("\t");
                    }
                    System.out.print(String.format("\t%s: %s", attribute.getName(),
                            attribute.getType().get(0)));
                }
                if (attribute.isPrimaryKey() == 1) {
                    System.out.print(" primarykey");
                }
                if (attribute.isUnique()) {
                    System.out.print(" unique");
                }
                if (attribute.isNotNull()) {
                    System.out.print(" notnull");
                }
                System.out.println();
            }
            if (printIndented) {
                System.out.print("\t");
            }
            System.out.println(String.format("Pages: %d", tables.get(name).getNumPages()));
            if (printIndented) {
                System.out.print("\t");
            }

            Table table = tables.get(name);
            int numPages = table.getNumPages(); // number of pages
            List<Integer> pageIds = table.getPages(); // list of pageIds
            int count = 0;
            for (int i = 0; i < numPages; i++) {
                count += manager.getBuffer().getPage(table, pageIds.get(i)).getContents().size();
            }
            System.out.println(String.format("Records: %d", count));
            return true;
        } catch (NoSuchTableException e) {
            System.err.println(e);
            return false;
        }
    }

    public boolean displaySchema(DisplaySchemaQuery query) {
        System.out.println(String.format("DB location: %s", dbLoc));
        System.out.println(String.format("Page Size: %d", pageSize));
        System.out.println(String.format("Buffer Size: %d", bufferSize));
        System.out.println("Tables: ");
        HashMap<String, SchemaTable> schema = catalog.getSchema();
        for (String name : schema.keySet()) {
            boolean printIndented = true;
            displayInfo(name, printIndented);
            System.out.println();
        }
        if (schema.keySet().size() == 0) {
            System.out.println("No tables to display");
        }
        return true; // should always happen only looking at existing tables.
    }

    public Catalog getCatalog() {
        return catalog;
    }

    public StorageManager getBuffer() {
        return this.manager;
    }

    public HashMap<String, Table> getTables() {
        return tables;
    }

    public HashMap<String, BPlusTree> getTrees() { return trees; }
}
