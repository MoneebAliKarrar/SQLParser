package QueryExceptions;

public class TableNotFoundException extends Exception {
        public TableNotFoundException(String tableName) {
            super("Table not found: " + tableName);
        }
    
}
