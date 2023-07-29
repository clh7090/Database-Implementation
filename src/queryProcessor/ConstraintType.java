package queryProcessor;

/**
 * @author Connor Hunter
 *
 * A constraint type is the useful for storing sql constraints like notnull or unique.
 */
public enum ConstraintType {

    PRIMARY_KEY("primarykey"),

    UNIQUE("unique"),

    NOTNULL("notnull");


    private String value;



    ConstraintType(String value) {
        this.value = value;
    }



    @Override
    public String toString() {
        return value;
    }
}
