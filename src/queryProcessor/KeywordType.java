package queryProcessor;

/**
 * @author Connor Hunter
 *
 * A keyword types makes it easy to detect which words are considered key words.
 */
public enum KeywordType {

    CREATE("create"),

    TABLE("table"),

    PRIMARY_KEY("primarykey"),

    SELECT("select"),

    FROM("from"),

    INSERT("insert"),

    INTO("into"),

    VALUES("values"),

    DISPLAY("display"),

    SCHEMA("schema"),

    INFO("info"),

    CHAR("char"),

    VARCHAR("varchar"),

    INTEGER("integer"),
    NULL("null"),

    TRUE("true"),

    FALSE("false"),
    
    DOUBLE("double"),

    NOTNULL("notnull"),

    UNIQUE("notnull"),

    DROP("drop"),

    ALTER("alter"),

    ADD("add"),

    DEFAULT("default"),

    BOOLEAN("boolean"),

    WHERE("where"),

    OR("or"),

    AND("and"),

    UPDATE("update"),

    SET("set"),

    ORDERBY("orderby");

    private String value;


    KeywordType(String value) {
        this.value = value;
    }


    @Override
    public String toString() {
        return value;
    }
}
