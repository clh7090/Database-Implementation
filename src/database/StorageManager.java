package database;

import bPlusTree.BPlusTree;
import exception.DuplicatePrimaryKeyException;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;

public class StorageManager {

    PageBuffer buffer;
    private Catalog catalog;

    private int pageSize;

    public StorageManager(Catalog catalog, int pageSize, String dbLoc, int bufferSize) {
        this.catalog = catalog;
        buffer = new PageBuffer(this.catalog, bufferSize, dbLoc, pageSize);
        this.pageSize = pageSize;
    }

    public ArrayList<Integer> insertRecordIntoPage(Table table, Record record) {

        if (table.getNumPages() == 0) {
            Page newPage = new Page(pageSize);
            table.getPages().add(newPage.getPageID());
            buffer.addPage(newPage, table);
        } else {
            for (int page : table.getPages()) {
                SchemaTable curSchema = catalog.getSchema().get(table.getTableName());
                Page current = buffer.getPage(table, page);
                for (int rIndex = 0; rIndex < current.getContents().size(); rIndex++) {
                    Record rec = current.getContents().get(rIndex);
                    // involves schema kailey says no no
                    if (catalog.compareKeys(curSchema, record, rec)) { // schema part
                        current.getContents().add(rIndex, record); // check if this inserts before it at that index may
                                                                   // do that
                        current.setNumRecords(current.getNumRecords() + 1);
                        if (current.isFull() == true) {
                            buffer.splitPage(table, current);
                        }
                        // these 3 lines added for indexing
                        ArrayList a = new ArrayList();
                        a.add(rIndex);
                        a.add(page);
                        return a;
                    }
                }
            }
        }

        // if it doesn't get inserted get last page in table
        Page last = buffer.getPage(table, table.getPages().size() - 1);
        int index = last.addRecord(record);
        if (last.isFull()) {
            buffer.splitPage(table, last);
        }
        // these 3 lines added for indexing
        ArrayList b = new ArrayList();
        b.add(index);
        b.add(last.getPageID());
        return b;
    }

    /**
     * returns list of copies of all altered records (drop) from the given table in
     * the form ArrayList<ArrayList<Object>>
     * 
     * @param table
     * @param attrIdx index of attribute to remove
     * @return
     */
    public ArrayList<ArrayList<Object>> getDropAlterRecords(Table table, int attrIdx) {
        ArrayList<ArrayList<Object>> allRecords = new ArrayList<>();
        // foreach value in the row
        ArrayList<Integer> pageIDs = table.getPages();
        // for each page
        for (int pageNum = 0; pageNum < pageIDs.size(); pageNum++) {
            Page page = buffer.getPage(table, pageNum);
            ArrayList<Record> records = page.getContents();
            // for each record
            for (Record record : records) {
                // removes value from record values
                ArrayList<Object> values = record.getData();
                values.remove(attrIdx);

                allRecords.add(values);
            }
        }

        return allRecords;
    }

    /**
     * returns list of copies of all altered records (add) from the given table in
     * the form ArrayList<ArrayList<Object>>
     * 
     * @param table
     * @param defaultVal
     * @return
     */
    public ArrayList<ArrayList<Object>> getAddAlterRecords(Table table, Object defaultVal) {
        ArrayList<ArrayList<Object>> allRecords = new ArrayList<>();
        // foreach value in the row
        ArrayList<Integer> pageIDs = table.getPages();
        // for each page
        for (int pageNum = 0; pageNum < pageIDs.size(); pageNum++) {
            Page page = buffer.getPage(table, pageNum);
            ArrayList<Record> records = page.getContents();
            // for each record
            for (Record record : records) {
                // adds default value to record values
                ArrayList<Object> values = record.getData();
                values.add(defaultVal);

                allRecords.add(values);
            }
        }

        return allRecords;
    }

    /**
     * returns array list of all records from specified table
     * 
     * @param table
     * @return
     */
    public ArrayList<Record> getAllRecordsInTable(Table table) {
        ArrayList<Record> allRecords = new ArrayList<>();
        // foreach value in the row
        ArrayList<Integer> pageIDs = table.getPages();
        // for each page
        for (int pageNum = 0; pageNum < pageIDs.size(); pageNum++) {
            Page page = buffer.getPage(table, pageNum);
            ArrayList<Record> records = page.getContents();
            // for each record
            for (Record record : records) {
                allRecords.add(record);
            }
        }
        return allRecords;
    }


    public void insertUsingRP(ArrayList<Integer> rp, Record record, Table table){
        if (table.getNumPages() == 0) {
            Page newPage = new Page(pageSize);
            table.getPages().add(newPage.getPageID());
            newPage.getContents().add(rp.get(0), record);
            newPage.setNumRecords(newPage.getNumRecords()+1);
            buffer.addPage(newPage, table);
            if (newPage.isFull()) {
                buffer.splitPage(table, newPage);
                // bPlusSplitRepair(newPage, table, database); if bplus split worked
            }
        } else {
            Page current = buffer.getPage(table, rp.get(1));
            current.getContents().add(rp.get(0), record);
            current.setNumRecords(current.getNumRecords() + 1);
            if (current.isFull() == true) {
                buffer.splitPage(table, current);
                // bPlusSplitRepair(newPage, table, database); if bplus split worked
            }
        }
    }

    public PageBuffer getBuffer() {
        return buffer;
    }

    private void bPlusSplitRepair(Page newPage, Table table, Database db) {
        int newPageID = newPage.getPageID();

        // gets index of primarykey
        int pkIdx = -1;
        Attribute attr = null;
        ArrayList<Attribute> attributes = catalog.getSchemaGivenTableName(table.getTableName());
        for (int i = 0; i < attributes.size(); i++) {
            attr = attributes.get(i);
            if (attr.isPrimaryKey() == 1) {
                pkIdx = i;
                break;
            }
        }

        BPlusTree tree = db.getTrees().get(table.getTableName());

        ArrayList<Record> records = newPage.getContents();
        for (int i = 0; i < records.size(); i++) {
            // update record pointer with new page ID
            Record rec = records.get(0);
            ArrayList<Integer> newRP =  db.searchBplus(rec.getData().get(pkIdx),tree , tree.getRoot(), attr.typeString(), false, true);
            newRP.set(0, newPageID);
            newRP.set(1, i);
        }

    }


}
