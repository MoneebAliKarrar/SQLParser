package Query;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import QueryExceptions.TableNotFoundException;
import QueryExceptions.invalidJoinConditionException;

public class Select {
    String tablename;
    String condition;
    String groupByClause;
    String joinCondition;
    String joinTableName;
    List<LinkedHashMap<String, Object>> result;
    public Select(String tablename,String condition,String groupByClause,String joinTableName,String joinCondition){
        this.tablename = tablename;
        this.condition = condition;
        this.groupByClause = groupByClause;
        this.joinTableName = joinTableName;
        this.joinCondition = joinCondition;
        }
    public  List<LinkedHashMap<String, Object>> execute(List<String> wantedColumns){
        List<LinkedHashMap<String, Object>> result = new ArrayList<>();
        try{
        if (FileHandling.checkexistance(tablename)) {
            try{
           List<String> lines = FileHandling.readFromFile(tablename);
           String columnNamesString = lines.get(0);
               for(String line : lines){
                if(!line.equals(columnNamesString)){
                    if(condition == null || condition.isEmpty()){
                        LinkedHashMap<String, Object> record = extractRecord(line, wantedColumns,columnNamesString);
                        result.add(record);
                        
                        
                    }else
                    if (WhereCondition.checkWhereCondition(line, condition,columnNamesString)) {
                        LinkedHashMap<String, Object> record = extractRecord(line, wantedColumns,columnNamesString);
                        result.add(record);
                        
                    }
                    
                  //  System.out.println(result);
                   
                    
                }
            
            }
             if (groupByClause != null && !groupByClause.isEmpty()) {
                        result = groupResults(result, wantedColumns, columnNamesString, groupByClause);
                    }
            if (joinTableName != null && joinCondition != null) {
                        result = executeJoin(result, wantedColumns);
                    }
            
            }catch(IOException e){
                e.printStackTrace();
            }
            
        } else {
            throw new TableNotFoundException(tablename);
        }}catch(TableNotFoundException e){
            System.out.println("Table not found: " + e.getMessage());
        }
        return result;
    }
    private static LinkedHashMap<String,Object> extractRecord(String line, List<String> wantedColumns,String columnNamesString){
    LinkedHashMap<String,Object> record = new LinkedHashMap<>();
    String[] values = line.split(",");
    String[] allColumns = columnNamesString.split(",");
    if (values.length == allColumns.length) {
        for (String columnName : wantedColumns) {
            int columnIndex = WhereCondition.findColumnIndex(columnName, allColumns);
            if (columnIndex != -1 && columnIndex < values.length) {
                String cleanedValue = values[columnIndex].trim().replaceAll("^\"|\"$", "");
                record.put(columnName.trim(), cleanedValue);
            }
        }
    }
        return record;
            }
            private static List<LinkedHashMap<String, Object>> groupResults(
    List<LinkedHashMap<String, Object>> result2,
    List<String> wantedColumns,
    String columnNamesString,
    String groupByClause
) {
    LinkedHashMap<List<String>, List<LinkedHashMap<String, Object>>> groupedData = new LinkedHashMap<>();
    
    String[] groupByColumns = groupByClause.trim().split("\\s*,\\s*");

    for (LinkedHashMap<String, Object> result : result2) {
        List<String> key = new ArrayList<>();
        for (String groupByColumn : groupByColumns) {
            key.add(String.valueOf(result.get(groupByColumn)));
        }

        groupedData.putIfAbsent(key, new ArrayList<>());

        groupedData.get(key).add(new LinkedHashMap<>(result));
    }

    List<LinkedHashMap<String, Object>> groupedResults = new ArrayList<>();

    for (List<LinkedHashMap<String, Object>> group : groupedData.values()) {
        LinkedHashMap<String, Object> combinedMap = new LinkedHashMap<>();
        for (LinkedHashMap<String, Object> resultMap : group) {
            for (Map.Entry<String, Object> entry : resultMap.entrySet()) {
                String columnName = entry.getKey();
                Object value = entry.getValue();
                boolean isGroupByColumn = Arrays.asList(groupByColumns).contains(columnName);
            if (!isGroupByColumn) {
                if (combinedMap.containsKey(columnName)) {
                    try {
                        if (value instanceof String && isParsableAsInteger((String) value)) {
                            int parsedValue = Integer.parseInt((String) value);
                            int currentValue = Integer.parseInt((String) combinedMap.get(columnName));
                            
                            combinedMap.put(columnName, currentValue + parsedValue);
                        } else {
                            combinedMap.put(columnName, String.valueOf(value));
                        }
                    } catch (ClassCastException e) {
                    
                    }
                } else {
                    combinedMap.put(columnName, value);
                }
            } else {
                combinedMap.put(columnName, value);
            }
            }
        }
        groupedResults.add(combinedMap);
    }
     
    return groupedResults;
}
private static boolean isParsableAsInteger(String input) {
    try {
        Integer.parseInt(input);
        return true;
    } catch (NumberFormatException e) {
        return false;
    }
}
private List<LinkedHashMap<String, Object>> executeJoin(
            List<LinkedHashMap<String, Object>> existingResult,
            List<String> wantedColumns
    ) {
        List<LinkedHashMap<String, Object>> result = new ArrayList<>();
        try {
            if (FileHandling.checkexistance(tablename) && FileHandling.checkexistance(joinTableName)) {
                // Read data from the primary table
                List<String> primaryTableData = FileHandling.readFromFile(tablename);
                // Read data from the joined table
                List<String> joinedTableData = FileHandling.readFromFile(joinTableName);
                
                // Iterate through each row in the primary table
                String[] joinConditionParts = joinCondition.split("=");

                String[] primaryJoinColumnParts = joinConditionParts[0].trim().split("\\.");
              //  String primaryTableName = primaryJoinColumnParts[0].trim();
                String primaryColumnName = primaryJoinColumnParts[1].trim();
                
                String[] joinedJoinColumnParts = joinConditionParts[1].trim().split("\\.");
                //String joinedTableName = joinedJoinColumnParts[0].trim();
                String joinedColumnName = joinedJoinColumnParts[1].trim();

                String[] primaryTableColumns = getColumnNamesString(tablename).split(",");
                String[] joinedTableColumns = getColumnNamesString(joinTableName).split(",");
                int primaryJoinColumnIndex = WhereCondition.findColumnIndex(primaryColumnName, primaryTableColumns);
                int joinedJoinColumnIndex = WhereCondition.findColumnIndex(joinedColumnName, joinedTableColumns);
                String primaryColumnNamesString = primaryTableData.get(0);
                String joinedColumnNamesString = joinedTableData.get(0);
            if (validateJoinCondition(primaryTableData, joinedTableData,joinCondition)) {
                for (int i = 1; i < primaryTableData.size(); i++) {
                    String primaryRow = primaryTableData.get(i);
                    String[] primaryValues = primaryRow.split(",");
                
                    // Iterate through each row in the joined table starting from index 1 (skipping the column names)
                   
                    for (int j = 1; j < joinedTableData.size(); j++) {
                        String joinedRow = joinedTableData.get(j);
                        String[] joinedValues = joinedRow.split(",");
                
                        // Check if the values in the join columns match
                        if (primaryValues[primaryJoinColumnIndex].equals(joinedValues[joinedJoinColumnIndex])) {
                            LinkedHashMap<String, Object> record = extractRecord(
                                primaryRow + "," + joinedRow,
                                wantedColumns,
                                primaryColumnNamesString + "," + joinedColumnNamesString
                            );
                
                            result.add(record);
                        }
                    }
                }
            }else{
                throw new invalidJoinConditionException("this is invalid join condition");
            }
    
            } else {
                throw new TableNotFoundException(tablename);
            }
        } catch (IOException | TableNotFoundException e) {
            e.printStackTrace(); 
        } catch (invalidJoinConditionException e) {
            System.out.println("Hey this is invalid join condition");
        }
        return result;
        
    }
    /* 
    private String getCombinedColumnNames() {

        String primaryTableColumns = getColumnNamesString(tablename);
        String joinedTableColumns = getColumnNamesString(joinTableName);
        return primaryTableColumns + "," + joinedTableColumns;
    }*/
    private String getColumnNamesString(String tableName) {
        try {
            List<String> lines = FileHandling.readFromFile(tableName);
            if (!lines.isEmpty()) {
                return lines.get(0);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return "";
    }
    private static boolean validateJoinCondition(List<String> primaryTableData,List<String>joinedTableData,String joinCondition ){

        String[] conditionParts = joinCondition.split("=");
        String[] primaryParts = conditionParts[0].trim().split("\\.");
        String[] joinedParts = conditionParts[1].trim().split("\\.");
    
        
        String primaryColumn = primaryParts[1].trim();
    
        String joinedColumn = joinedParts[1].trim();
    
        // Assuming the first row contains column names
        String primaryColumnNamesString = primaryTableData.get(0);
        String joinedColumnNamesString = joinedTableData.get(0);
    
        // Check if the join columns exist in their respective tables
        boolean primaryColumnExists = primaryColumnNamesString.contains(primaryColumn);
        boolean joinedColumnExists = joinedColumnNamesString.contains(joinedColumn);
    
        // If the columns exist in both tables, validate their data type (you may customize this part)
        if (primaryColumnExists && joinedColumnExists) {
            String primaryColumnType = getColumnType(primaryColumnNamesString, primaryColumn);
            String joinedColumnType = getColumnType(joinedColumnNamesString, joinedColumn);
    
            // Example: Check if the data types are the same
            return primaryColumnType.equals(joinedColumnType);
        } else {
            return false;
        }
    }
    private static String getColumnType(String columnNamesString, String columnName) {
        String[] columns = columnNamesString.split(",");
        for (int i = 0; i < columns.length; i++) {
            if (columns[i].trim().equals(columnName)) {
                
                String[] parts = columnNamesString.split("\\s+");
                if (parts.length > i + 1) {
                    return parts[i + 1].trim(); 
                }
            }
        }
        
        return "";
    }
}
