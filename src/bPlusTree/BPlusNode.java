package bPlusTree;

import java.util.ArrayList;

public class BPlusNode {

    private boolean isLeaf;

    private boolean isInternal;

    private boolean isRoot;

    private int numOfRecPointers;

    private ArrayList<Object> searchKeys;

    private BPlusNode parent;

    private ArrayList<BPlusNode> children;

    private BPlusNode neighbor;

    private ArrayList<ArrayList<Integer>> recordPointers;

    /**
     * Creates a B+ tree node. The node will record if the node is a root, leaf, or internal node:
     *      EX. if leaf node:
     *          isRoot: false
     *          isInternal: false
     *          isLeaf: true
     *      recordPointers will be set to null. It is an arraylist of arraylist that contains page # in index 0,
     *          index # on page for index 1. Only used for LEAF NODES
     *      searchKeys will be set to null on creation. It will be an arraylist of objects that contain the specific
     *          attribute type of the primary key.
     *      numOfRecordPointers keeps track of how many record pointers the node has
     *      numOfChildPointers keeps track of how many internal node pointers there are
     */
    public BPlusNode(boolean isRoot, boolean isLeaf, boolean isInternal){
        this.isRoot = isRoot;
        this.isLeaf = isLeaf;
        this.isInternal = isInternal;

        this.numOfRecPointers = 0;
        this.searchKeys = new ArrayList<>();
        this.parent = null;
        this.children = new ArrayList<>();
        this.recordPointers = new ArrayList<>();
        this.neighbor = null;
    }

    public int getNumOfRecPointers() {
        return numOfRecPointers;
    }

    public ArrayList<Object> getSearchKeys() {
        return searchKeys;
    }

    public ArrayList<ArrayList<Integer>> getRecordPointers() {
        return recordPointers;
    }

    public void addRP(ArrayList<Integer> rp, int idx) {
        if(idx == -1) { // add to end by default
            recordPointers.add(rp);
        }
        else { // add at specific spot
            recordPointers.add(idx, rp);
        }
        numOfRecPointers++;
    }

    public void addSearchKey(Object o, int idx) {
        if(idx == -1) { // add to end by default
            searchKeys.add(o);
        }
        else { // add at specific spot
            searchKeys.add(idx, o);
        }
    }

    public void setParent(BPlusNode parent) {
        this.parent = parent;
    }

    public ArrayList<BPlusNode> getChildren() {
        return children;
    }


    public BPlusNode getNeighbor() {
        return neighbor;
    }

    public boolean isLeaf() {
        return isLeaf;
    }

    public boolean isRoot() {
        return isRoot;
    }

    public boolean isInternal() {
        return isInternal;
    }

    public void setLeaf(boolean leaf) {
        isLeaf = leaf;
    }

    public void setInternal(boolean internal) {
        isInternal = internal;
    }

    public void setRoot(boolean root) {
        isRoot = root;
    }

    public void setRecordPointers(ArrayList<ArrayList<Integer>> recordPointers) {
        this.recordPointers = recordPointers;
    }

    public void setSearchKeys(ArrayList<Object> searchKeys) {
        this.searchKeys = searchKeys;
    }

    public void setNumOfRecPointers(int numOfRecPointers) {
        this.numOfRecPointers = numOfRecPointers;
    }

    public BPlusNode getParent() {
        return parent;
    }

    public void setNeighbor(BPlusNode neighbor) {
        this.neighbor = neighbor;
    }

    public void setChildren(ArrayList<BPlusNode> children) {
        this.children = children;
    }

    @Override
    public String toString() {
        return "BPlusNode{" +
                ", searchKeys=" + searchKeys +
                '}';
    }
}
