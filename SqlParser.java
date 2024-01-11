import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import Query.Delete;
import Query.FileHandling;
import Query.Insert;
import Query.Select;
import Query.Update;
import QueryExceptions.TableAlreadyExists;
import QueryExceptions.TableNotFoundException;

public class SqlParser {
    public static void main(String[] args) throws Exception {
        Scanner scanner = new Scanner(System.in);
        String userInput;
        System.out.println("Welcome to the MySQL monitor.  Commands end with ';' / type 'exit' to EXIT ");
        System.out.println("Please enter an sql command");
        while (true) {
            System.out.print("mysql> ");
            userInput = scanner.nextLine().trim();
            if (userInput.equals("exit")) {
                scanner.close();
                break;
            }
            System.out.println(parseAndExecute(userInput)); ;
        }
        System.out.println("Exiting the database application.");
    }

    public static String parseAndExecute(String userInput) {
        final String CREATE_REGEX = "^create\\s+table\\s+(\\w+)\\s*\\(([^)]+)\\)\\s*;";
        final String DROP_REGEX = "^\\s*drop\\s+table\\s+([a-zA-Z_][a-zA-Z0-9_]*)\\s*;\\s*$";
        final String DELETE_REGEX = "^\\s*delete\\s+from\\s+([a-zA-Z_][a-zA-Z0-9_]*)\\s*(?:where\\s+(.+))?\\s*;\\s*$";
        final String INSERT_REGEX = "^\\s*insert\\s+into\\s+([a-zA-Z_][a-zA-Z0-9_]*)\\s*\\(([^,]+(?:\\s*,\\s*[a-zA-Z_][a-zA-Z0-9_]*)*)\\)\\s*values\\s*\\(([^,]+(?:\\s*,\\s*[^,]+)*)\\)\\s*;\\s*$";
        final String SELECT_REGEX = "\\bSELECT\\b\\s+(.*?)\\bFROM\\b\\s+(.*?)(?:\\s+\\bWHERE\\b\\s+(.*?))?(?:\\s+\\bJOIN\\b\\s+(.*?)\\s+\\bON\\b\\s+(.*?))?(?:\\s+\\bGROUP\\b\\s+\\bBY\\b\\s+(.*?))?\\s*;";
        final String UPDATE_REGEX = "^update (\\w+) set (.+?)(?: where (\\w+ = \\S+);)?";
         
        Pattern CREATE_PATTERN = Pattern.compile(CREATE_REGEX, Pattern.CASE_INSENSITIVE);
        Pattern DROP_PATTERN = Pattern.compile(DROP_REGEX, Pattern.CASE_INSENSITIVE);
        Pattern DELETE_PATTERN = Pattern.compile(DELETE_REGEX, Pattern.CASE_INSENSITIVE);
        Pattern INSERT_PATTERN = Pattern.compile(INSERT_REGEX, Pattern.CASE_INSENSITIVE);
        Pattern SELECT_PATTERN = Pattern.compile(SELECT_REGEX, Pattern.CASE_INSENSITIVE);
        Pattern UPDATE_PATTERN = Pattern.compile(UPDATE_REGEX, Pattern.CASE_INSENSITIVE);

        Matcher createMatcher = CREATE_PATTERN.matcher(userInput);
        Matcher dropMatcher = DROP_PATTERN.matcher(userInput);
        Matcher deleteMatcher = DELETE_PATTERN.matcher(userInput);
        Matcher insertMatcher = INSERT_PATTERN.matcher(userInput);
        Matcher selectMatcher = SELECT_PATTERN.matcher(userInput);
        Matcher updateMatcher = UPDATE_PATTERN.matcher(userInput);

        if (createMatcher.matches()) {
            String tablename = createMatcher.group(1);
            String columnNamesString = createMatcher.group(2);
            if (!FileHandling.checkexistance(tablename)) {
                executeCreate(tablename, columnNamesString);
                return "Table created successfully: " + tablename;
            } else {
                return "Table Already Exists: " + tablename;
            }
           

        } else if (dropMatcher.matches()) {
            String tablename = dropMatcher.group(1);
            return  executeDrop(tablename);
        } else if (deleteMatcher.matches()) {
            String tablename = deleteMatcher.group(1);
            String whereClauseString = deleteMatcher.group(2);
            return  executeDelete(tablename, whereClauseString);
        } else if (insertMatcher.matches()) {
            String tablename = insertMatcher.group(1);
            String columnNamesString = insertMatcher.group(2);
            String valuesString = insertMatcher.group(3);
            return executeInsert(tablename, columnNamesString, valuesString);
        } else if (selectMatcher.matches()) {
            String tablename = selectMatcher.group(2);
            String selectedColumnsString = selectMatcher.group(1);
            String whereClauseString = selectMatcher.group(3);
            String groupByClause = selectMatcher.group(6);
            String joinTableName = selectMatcher.group(4);
            String joinCondition = selectMatcher.group(5);     
            executeSelect(tablename, selectedColumnsString, whereClauseString,groupByClause,joinTableName,joinCondition);
            return executeSelect(tablename, selectedColumnsString, whereClauseString,groupByClause,joinTableName,joinCondition);
        } else if (updateMatcher.matches()) {
            String tablename = updateMatcher.group(1);
            String setClauseString = updateMatcher.group(2);
            String whereClauseString = updateMatcher.group(3); 
            return executeUpdate(tablename, setClauseString, whereClauseString);
        }else {
            return"Error: Unable to parse the SQL command. Please check the syntax.";
        }
    }

    private static void executeCreate(String tablename, String columnNamesString) {
        try {
            FileHandling.createTableFile(tablename);
            String[] columnNamesWithtype = columnNamesString.split("\\s*,\\s*");
            List<String> columnNames = new ArrayList<>();
            for (String columnNameWithType : columnNamesWithtype) {
                String[] parts = columnNameWithType.trim().split("\\s+");
                String columnName = parts[0].trim();
                columnNames.add(columnName);
            }
            FileHandling.writeColumnsToFile(tablename, columnNames);
        } catch (TableAlreadyExists e) {
            System.out.println("TABLE ALREADY EXIST: "+e.getMessage());
        }
    }

    private static String executeDrop(String tablename) {
        try {
            
            return FileHandling.dropTableFile(tablename);
        } catch (TableNotFoundException e) {
            return "TABLE DOES'NT EXIST: ";
        }

    }

    private static String executeDelete(String tablename, String whereClauseString) {
        Delete delete = new Delete(tablename, whereClauseString);
        return delete.execute();
    }

    private static String executeInsert(String tablename, String columnNamesString, String valuesString) {
        Insert insert = new Insert(tablename);
        String[] columnNames = columnNamesString.trim().split("\\s*,\\s*");
        String[] valuesArray = valuesString.trim().split("\\s*,\\s*");
        Object[] values = new Object[valuesArray.length];
        for (int i = 0; i < values.length; i++) {
            values[i] = parseValue(valuesArray[i]);
        }
        if (columnNamesAreValid(tablename, columnNames)) {
            if (columnNames.length == values.length) {
                String output =  insert.execute(values);
                return output;
            }else{
                return "Wrong number of values";
            }
        } else {
           return "SORRY THE COLUMNS YOU PROVIDED ARE NOT VALID" ;
        }

    }

    private static String executeSelect(String tablename, String selectedColumnsString, String whereClauseString,String groupByClause,String joinTableName,String joinCondtion) {
        Select select = new Select(tablename, whereClauseString,groupByClause,joinTableName,joinCondtion);
        String[] allSelectedColumns = selectedColumnsString.split(",");
        List<String> columnNames = new ArrayList<>();
        for (String name : allSelectedColumns) {
            columnNames.add(name);
        }
        StringBuilder result = new StringBuilder();
        List<LinkedHashMap<String, Object>> selectedColumns = select.execute(columnNames);
       for (String columnName : columnNames) {
            result.append(columnName).append("\t");
        }  
        result.append("\n");
                for (LinkedHashMap<String, Object> columnMap : selectedColumns) {
                    for (Object columnValue : columnMap.values()) {
                        result.append(columnValue).append("\t");
                    }
                    result.append("\n");
            }
            
           return result.toString();
        }
    private static String executeUpdate(String tablename, String setClauseString, String whereClauseString) {
        String[] columnsNamesWithValues = setClauseString.split("\\s*,\\s*");
        Map<String, Object> setvalues = new HashMap<>();
        for (String columnNameWithValue : columnsNamesWithValues) {
            String[] parts = columnNameWithValue.trim().split("=");
            String columnName = parts[0].trim();
            Object newValue;
            try {
                newValue = Integer.parseInt(parts[1].trim());
            } catch (NumberFormatException e) {
                newValue = parts[1].trim();
            }
            setvalues.put(columnName, newValue);
        }

        Update update = new Update(tablename, setvalues, whereClauseString);
        return update.execute();
    }

    private static Object parseValue(String value) {
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e1) {
            try {
                return Double.parseDouble(value);
            } catch (NumberFormatException e2) {
                return value;
            }

        }
    }

    private static boolean columnNamesAreValid(String tablename, String[] columnNames) {
        try {
            List<String> lines = FileHandling.readFromFile(tablename);
            if (lines.size() > 0) {
                String columnsLine = lines.get(0);
                String[] columns = columnsLine.split(",");
                return Arrays.asList(columns).containsAll(Arrays.asList(columnNames));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    
}
