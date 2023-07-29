package database;

import java.util.ArrayList;
import java.util.Comparator;

public class Record implements Comparator<Object> {

    // dynamic list that can hold multiple types 
    private ArrayList<Object> data;

    private ArrayList<Integer> bitMap;

    public Record(ArrayList<Object> queryData){
        data = queryData;
        bitMap = new ArrayList<>();
        for(Object o : queryData){ // nulls being stored 0
            if(o == null){
                bitMap.add(0);
            }else {
                bitMap.add(1);
            }
        }
    }

    public ArrayList<Object> getData() {
        return data;
    }

    public ArrayList<Integer> getBitMap() {
        return bitMap;
    }

    /**
     * Compares Objects in record data list. Used for insertion sort of records in Database.java. Compare function will
     *      first look to see what type of objects is being sorted and then type cast them and compare.
     *
     * @param o1 Object1 being compared
     * @param o2 Object 2 being compared
     * @return integer; 0 if same,-1 if o1 < o2; 1 if o1 > o2
     */
    @Override
    public int compare(Object o1, Object o2) {
        if (o1 instanceof String && o2 instanceof String) {
            String str1 = (String) o1;
            String str2 = (String) o2;

            return str2.compareToIgnoreCase(str1);

            //null cases
        } else if (o1 == null && o2 == null) {
            return 0;
        } else if (o1 == null) {
            return -1;
        } else if (o2 == null) {
            return 1;
        } else if (o1 instanceof Integer && o2 instanceof Integer) {
            int i1 = (Integer) o1;
            int i2 = (Integer) o2;

            return i2 - i1;
        } else {
            double d1 = (double) o1;
            double d2 = (double) o2;
            double dr = d2 - d1;
            return (int) dr;
        }
    }

}
