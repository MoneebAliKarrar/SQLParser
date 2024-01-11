package Query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import QueryExceptions.RecordNotFoundException;
import QueryExceptions.TableNotFoundException;

public class Update {
    String tablename;
    Map<String, Object> setvalues;
    String condition;

    public Update(String tablename, Map<String, Object> setvalues, String condition) {
        this.tablename = tablename;
        this.setvalues = setvalues;
        this.condition = condition;

    }

    public String execute() {
        boolean updateOccured = false;
        try{
        if (FileHandling.checkexistance(tablename)) {

            try {

                List<String> updatedlines = new ArrayList<>();

                List<String> lines = FileHandling.readFromFile(tablename);
                String columnNamesString = lines.get(0);
                String[] columnNames = columnNamesString.split(",");
                if (lines.size() == 1) {
                    return "THERE IS NO RECORDS TO DELETE" ;
                }
                if (condition == null || condition.isEmpty()) {
                    if (!lines.isEmpty()) {
                        for (int i = 0; i < lines.size(); i++) {
                            String line = lines.get(i);
                            if (i == 0) {
                                updatedlines.add(line);
                                continue;
                            } 
                        line = updateLine(line, setvalues,columnNames);
                            updatedlines.add(line);
                    }}
                    updateOccured = true;
                }else{
                for (String line : lines) {    
                    if (WhereCondition.checkWhereCondition(line, condition,columnNamesString)) {
                        line = updateLine(line, setvalues,columnNames);
                        updateOccured = true;
                    }
                    updatedlines.add(line);
                }
            }
                
                if (updateOccured) {
                    FileHandling.writeToFile(tablename, updatedlines);
                    return "Table updated successfully.";
                } else {
                    throw new RecordNotFoundException("Invalid Record");
                
                }

            } catch (IOException e) {
                return "Error updating table. " + e.getMessage();
            }catch(RecordNotFoundException e){
                return "INVALID CONDITION "+e.getMessage();
            }
        } else {
            throw new TableNotFoundException(tablename);
        }}catch(TableNotFoundException e){
            return "Table not found: " + e.getMessage();
        }
    }

    private static String updateLine(String line, Map<String, Object> setvalues,String[] columnNames) {
        String[] columns = line.split(",");
        for (Map.Entry<String, Object> entry : setvalues.entrySet()) {
            String columnName = entry.getKey();
            Object newValue = entry.getValue();

            int columnIndex = WhereCondition.findColumnIndex(columnName, columnNames);
            if (columnIndex != -1) {
                columns[columnIndex] = String.valueOf(newValue);
            }
        }
        return String.join(",", columns);

    }
}
