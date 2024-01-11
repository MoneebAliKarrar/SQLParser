package Query;



public class WhereCondition {

    private WhereCondition() {
        // Private constructor to prevent instantiation
    }
    public static boolean checkWhereCondition(String line, String condition,String columnNames) {
        String[] whereParts = condition.trim().split("=");
        if (whereParts.length != 2) {
            return false;
        }
        String columnName = whereParts[0].trim();
        String expectedValue = whereParts[1].trim();

        String[] columnsNames = columnNames.split(",");
        String[] values = line.split(",");

        int columnIndex = findColumnIndex(columnName, columnsNames);

        return columnIndex != -1 && values[columnIndex].trim().equals(expectedValue);
    }
    public static int findColumnIndex(String columnName, String[] columns) {
        for (int i = 0; i < columns.length; i++) {
            if (columns[i].trim().equalsIgnoreCase(columnName.trim())) {
                return i;
            }
        }
        return -1; //retrun -1 if column not found
    }
    
}
