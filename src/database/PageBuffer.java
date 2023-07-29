package database;


import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class PageBuffer {

    //Command line argument to be used --- # of pages
    private int bufferSize;
    //Buffer Arraylist
    private ArrayList<Page> pages = new ArrayList<>();
    // list of table objects that correspond to each page in pages list
    private ArrayList<Table> tables = new ArrayList<>();

    private Catalog catalog;

    private String dbLoc;

    private int pageSize;

    /**
     PageBuffer - Constructor for buffer
     */
    public PageBuffer(Catalog catalog, int bufferSize, String dbLoc, int pageSize){
        this.catalog = catalog;
        this.bufferSize = bufferSize;
        this.dbLoc = dbLoc;
        this.pageSize = pageSize;
    }

    public ArrayList<Page> getPageBuffer() {
        return pages;
    }

    /**
     * Gets a page either within the buffer or finds the page and writes it to the buffer. If buffer is full,
     * Least Recently used gets written to the disk
     *
     * @param table     Used to access read/write of pages
     * @param pageID   Page ID number we are looking for
     * @return          returns either the page found in buffer (already existing) or new Page created
     */
    public Page getPage(Table table, int pageID){

        //If the page we are looking for already exists, we remove and re-add to the buffer for LRU
        //returns the page found
        for( int i = 0; i < pages.size(); i++){
            Page page = pages.get(i);
            // if pageID is in pages and the corresponding table matches the given one:
            if(page.getPageID() == pageID && table.equals(tables.get(i))){
                removePage(i);
                addPage(page, table);
                return page;
            }
        }

        //Creates null newPage to be read
        Page newPage = new Page(pageSize);

        //TODO: may need fixes
        try {
            newPage = readPage(table.getTableID(), pageID);
        } catch (IOException e) {
            e.printStackTrace();
        }

        //adds newPage to the buffer
        addPage(newPage, table);

        return newPage;
    }

    public Record readRecord(ByteBuffer bytebuff, ArrayList<Attribute> attributes){
        Integer bitMapLen = bytebuff.getInt();
        ArrayList<Integer> bitmap = new ArrayList<>();
        for (int i = 0; i < bitMapLen; i++){
            Integer num = bytebuff.getInt();
            bitmap.add(num);
        }

        ArrayList<Object> record = new ArrayList<>();
        int k = 0;
        for (Attribute current : attributes) {
            ArrayList<String> type = current.getType();
            int typeCheck = this.catalog.getTypeKey(type.get(0));
            switch (typeCheck) {
                case (0): // int
                    if(bitmap.get(k) == 0){//null case
                        record.add(null);
                    }else {
                        record.add(bytebuff.getInt());
                    }
                    break;
                case (1): // double
                    if(bitmap.get(k) == 0){//null case
                        record.add(null);
                    }else {
                        record.add(bytebuff.getDouble());
                    }
                    break;
                case (2): // bool
                    if(bitmap.get(k) == 0){
                        record.add(null);
                    }else {
                        int flag = bytebuff.getInt();

                        if (flag == 1) {
                            record.add(true);
                        } else {
                            record.add(false);
                        }
                    }
                    break;
                case (3): // char
                    if(bitmap.get(k) == 0){
                        record.add(null);
                    }else {
                        int len = bytebuff.getInt();
                        StringBuilder sb = new StringBuilder();
                        for (int m = 0; m < len; m++){
                            sb.append(bytebuff.getChar());
                        }
                        record.add(sb.toString());
                    }
                    break;
                case (4): // varchar
                    if(bitmap.get(k) == 0){
                        record.add(null);
                    }else {
                        int len = bytebuff.getInt();
                        StringBuilder sb = new StringBuilder();
                        for (int m = 0; m < len; m++){
                            sb.append(bytebuff.getChar());
                        }
                        record.add(sb.toString());
                    }
                    break;
            }
            k++;
        }
        return new Record(record);
    }

    public Page readPage(int tableID, int offset) throws IOException {
        byte[] pagebytes = new byte[this.pageSize];
        RandomAccessFile file = new RandomAccessFile(String.format("%s\\%d", dbLoc, tableID), "r");
        // skipping initial number of pages and other pages that come before
        file.seek(Integer.BYTES + (offset*this.pageSize));
        // using pageID to move to beginning of correct page
        // reading 1 page
        file.read(pagebytes);
        ByteBuffer bytebuff = ByteBuffer.wrap(pagebytes);
        ArrayList<Record> records = new ArrayList<>();

        int numRecs = bytebuff.getInt();
        String tableName = this.catalog.getTableName(tableID);
        SchemaTable currentTable = this.catalog.getSchema().get(tableName);
        ArrayList<Attribute> attributes = currentTable.getAttributes();

        for(int r = 0; r < numRecs; r++){
            Record record = readRecord(bytebuff, attributes);
            records.add(record);
        }

        return new Page(records,numRecs, pageSize);
    }

    public void writePageIDSSize(int size, int tableID) throws IOException {
        String filepath = String.format("%s\\%d", dbLoc, tableID);
        RandomAccessFile raf = new RandomAccessFile(filepath, "rw");
        raf.seek(0);
        raf.writeInt(size);
        raf.close();
    }

    public int readPageIDSSize(int tableID) throws IOException {
        String filepath = String.format("%s\\%d", dbLoc, tableID);
        RandomAccessFile raf = new RandomAccessFile(filepath, "rw");
        // if no records, num is 0 and dont read
        int numPageinTable = 0;
        if(raf.length() != 0) { // else read
            raf.seek(0);
            numPageinTable = raf.readInt();
        }
        raf.close();
        return numPageinTable;
    }

    public void writeRecord(RandomAccessFile file, Record record, ArrayList<Attribute> schemaAttrs) throws IOException {
        // write length of bitmap
        // write bitmap itself
        Integer bitMapLen = record.getBitMap().size();
        file.writeInt(bitMapLen);
        for (Integer j : record.getBitMap()){
            file.writeInt(j);
        }

        ArrayList<Object> data = record.getData();
        for(int i = 0; i < data.size(); i++){
            Attribute currentAtt = schemaAttrs.get(i);
            Object datatype = data.get(i);
            ArrayList<String> type = currentAtt.getType();
            int typeCheck = this.catalog.getTypeKey(type.get(0));
            switch (typeCheck) { // TODO edge case missing nulls
                case (0): //int
                    if(!(datatype == null)){
                        int integer = (Integer)datatype;
                        file.writeInt(integer);
                    }
                    break;
                case (1): // double
                    if(!(datatype == null)) {
                        double doble = (Double) datatype;
                        file.writeDouble(doble);
                    }
                    break;
                case (2): // boolean
                    if(!(datatype == null)) {
                        boolean b = (boolean) datatype;
                        if (b) {
                            file.writeInt(1);
                        } else {
                            file.writeInt(0);
                        }
                    }
                    break;
                case (3): // char
                case (4): // varchar
                    if(!(datatype == null)) {
                        String string = (String) datatype;
                        int len = string.length();
                        file.writeInt(len);
                        file.writeChars(string);
                    }
                    break;
            }
        }
    }

    public void writePage(Page page, int tableID) throws IOException{
        String filepath = String.format("%s\\%d", dbLoc, tableID);
        RandomAccessFile pageWrite = new RandomAccessFile(filepath, "rw");
        int pageN = page.getPageID();
        int pageSize = page.getPageSize();


       if(pageN > 0){
           pageWrite.seek(pageSize *pageN + Integer.BYTES);
       } else {
        pageWrite.seek(Integer.BYTES);
       }


        //Number of records written
        pageWrite.writeInt(page.getNumRecords());

        String tableName = this.catalog.getTableName(tableID);
        SchemaTable currentTable = this.catalog.getSchema().get(tableName);
        ArrayList<Attribute> attributes = currentTable.getAttributes();
        ArrayList<Record> records = page.getContents();

        //Writes each record
        for(Record record: records){
            writeRecord(pageWrite, record, attributes);
        }

        pageWrite.close();
    }

    // public void writeAllPages(Table table) throws IOException {
    //     ArrayList<Integer> pageIds = table.getPages();
    //     ArrayList<Page> pages = new ArrayList<>();
    //     int numPages = table.getNumPages();

    //     for(int p =0; p < numPages; p++){
    //         Page page = getPage(table, pageIds.get(p));
    //         pages.add(page);
    //     }

    //     int tid = table.getTableID();
    //     for(Page pa: pages){
    //         writePage(pa, tid);
    //     }
    // }

    /**
     * checks if page buffer is full
     * writes out the least recently used page and removes it from the list
     */
    public boolean writeIfFull(Table table) {
        if (pages.size() > bufferSize) {

            ArrayList<Integer> pageIDS = table.getPages();

            try {
                writePageIDSSize(pageIDS.size(), table.getTableID());
                writePage(pages.get(0), tables.get(0).getTableID());
            } catch (IOException e) {
                e.printStackTrace();
            }
            // remove page and its table from page buffer
//            removePage(0); no
            return true;
        }
        return false;
    }

    /**
     * adds given page and table to lists
     * @param page
     * @param table
     */
    public void addPage(Page page, Table table) {
        pages.add(page);
        tables.add(table);
        writeIfFull(table);
    }

    /**
     * removes page and table at given index
     * @param idx
     */
    public void removePage(int idx) {
        pages.remove(idx);
        tables.remove(idx);
    }

    /**
     removes table from page buffer and all associated pages
     @param tableName
     */
    public void removeTable(String tableName){
        Iterator<Page> pageIterator = pages.iterator();
        Iterator<Table> tableIterator = tables.iterator();

        while(pageIterator.hasNext() && tableIterator.hasNext()){
            Page page = pageIterator.next();
            Table table = tableIterator.next();

            if(table.getTableName().equals(tableName)){
                pageIterator.remove();
                tableIterator.remove();
            }
        }
    }
    /**
     * splits given page
     * makes a new page for the other half of the split and puts it in the page buffer
     * inserts new page ID into the table after given page
     * @param table
     * @param page
     */
    public void splitPage(Table table, Page page) {
        Page newPage = new Page(pageSize);
        int curPageSize = page.getPageSize(); // current size of overflowing page;
        int idealSize = curPageSize/2; // ideal size of daughter pages

        // calculating where page will be split
        int halfsize1 = 0; //pagesize of daughter page 1
        int halfNum = 0; // num of records in first page
        ArrayList<Record> records = page.getContents();
        for(Record record : records) {
            halfsize1+=page.getRecordSize(record.getData());
            // adding null arraylist to record size
            halfsize1+=Integer.BYTES + (record.getBitMap().size()*Integer.BYTES);
            halfNum++;
            if(halfsize1 >= idealSize) {
                break;
            }
        }
        // split records of given page into two lists
        ArrayList<Record> firstHalf = new ArrayList<>(halfNum);
        ArrayList<Record> secondHalf = new ArrayList<>(page.getNumRecords() - halfNum);

        for (int i = 0; i < halfNum; i++) {
            firstHalf.add(records.get(i));
        }

        for (int i = halfNum; i < page.getNumRecords(); i++) {
            secondHalf.add(records.get(i));
        }

        page.setContents(firstHalf);
        newPage.setContents(secondHalf);

        newPage.setPageID(table.getNumPages());
        for(int j : table.getPages()){
            if(j == page.getPageID()){
                table.getPages().add(j+1, newPage.getPageID());
                break;
            }
        }

        for( int i = 0; i < pages.size(); i++){
            Page page1 = pages.get(i);
            // if pageID is in pages and the corresponding table matches the given one:
            if(page1.getPageID() == page.getPageID() && table.equals(tables.get(i))){
                removePage(i);
                addPage(page1, table);
                break;
            }
        }
        addPage(newPage, table);
    }

    /**
     * If database is shut down, loop through the buffer and write each page to disk. Then clear the buffer.
     */
    public void purgeBuffer() throws IOException {
        for(int p = 0; p < pages.size(); p++){
            Table t = tables.get(p);
            ArrayList<Integer> pageIDS = t.getPages(); // arraylist of pageIDs in this table
            writePageIDSSize(pageIDS.size(), t.getTableID()); // first integer in memory for num Pages
            int tid = t.getTableID();
            writePage(pages.get(p), tid);
        }

    }
}
