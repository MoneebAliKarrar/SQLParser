package QueryExceptions;

public class TableAlreadyExists extends Exception {
    public TableAlreadyExists(String tableName) {
        super("Table Already Exists " + tableName);
    }
}
