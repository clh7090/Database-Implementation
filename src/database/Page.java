package database;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Page {

    private int numRecords;

    private ArrayList<Record> contents;

    private int pageID;

    private int pageSize;

    // Constructor 2
    // read page
    public Page(ArrayList<Record> page, int num, int pageSize) {
        numRecords = num;
        contents = page;
        this.pageSize = pageSize;
    }

    // Constructor 1
    // initial creation of a page
    public Page(int pageSize) {
        numRecords = 0;
        contents = new ArrayList<>();
        this.pageSize = pageSize;
    }

    // adds a record to the page.
    public int addRecord(Record record) {
        contents.add(record);
        numRecords++;
        return numRecords - 1;
    }

    public int getPageID() {
        return pageID;
    }

    public int getPageSize() {
        return pageSize;
    }

    public boolean isFull() {
        int size = calculatePageSize();
        if(size > pageSize){
            return true;
        }
        return false;
    }


    public ArrayList<Record> getContents() {
        return contents;
    }


    public int getNumRecords() {
        return numRecords;
    }

    public int calculatePageSize() {
        int cps = 0;
        for(Record r: this.contents){
            ArrayList<Object> data = r.getData();
            cps+=getRecordSize(data);
        }
        return cps;
    }

    public int getRecordSize(ArrayList<Object> data) {
        int recSize = 0;
        for(Object o: data){

            if(o instanceof Integer){
                recSize+= Integer.BYTES;
            }

            if(o instanceof Double){
                recSize+= Double.BYTES;
            }

            //Stored as a int flag
            if(o instanceof Boolean){
                recSize+= Integer.BYTES;
            }

            //char and varchar length time char
            if(o instanceof String){
                String s = (String) o;
                //int len = Integer.parseInt(s);
                int len = s.length();
                recSize+= len*Character.BYTES;
            }
        }
        recSize+=Integer.BYTES + (data.size()*Integer.BYTES); // accounting for null byte arraylist
        return recSize;
    }


    public void setPageID(int pageID) {
        this.pageID = pageID;
    }


    public void setContents(ArrayList<Record> contents) {
        this.contents = contents;
        this.numRecords = contents.size();
    }


    public void setNumRecords(int numRecords) {
        this.numRecords = numRecords;
    }



}
