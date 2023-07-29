# Database-System-Implementation-Project
### How to run it?

the application can be built with the command in a terminal in the current project directory (~/src):

```
$ javac Main.java
```
and run with:
```
$ java Main.java <db loc> <page size> <buffer size>
```
Phase 1:
No known bugs. Everything should work as intended after issues fixed in Phase 2.

Phase 2:
No known bugs. Everything should work as intended.

Phase 3:
No known bugs. Everything should work as intended.

Phase 4:
for insert it is almost fully working except for changing record pointers after split. Writing to hardware and resetting the db also works with insert if no splits occur. 
<b>NOTE: INSERTING LARGE AMOUNTS OF VALUES MAY BREAK THE PROGRAM BECAUSE OUT BPLUS TREE REPAIR FOR RECORD POINTERS DOES NOT WORK</b>



<ol>
    <li>
         BPlusTree <i>itself</i> is mot written to memory
    </li>
    <li>
         repairBplusTree() reassign pointers after split 
    </li>
    <li>
        delete does not work fully, recursion is broken
    </li>
    <li>
        delete record pointer and search for the pointers works from leaf nodes. 
    </li>
    <li>
        delete merge and borrow should work for the most part (may be an missing edge case or a few )
        This causes the whole thing to not work.
    </li>
    <li>
        delete internal nodes can merge but behavior if merging is not possible i.e., when you need to bring down a search 
        key from the parent to make a new internal node
    </li>
    <li>
        updating is normally deleting the old value and insert the new value but we couldn't get it to work properly 
        because insert was not working. 
    </li>
    
</ol>