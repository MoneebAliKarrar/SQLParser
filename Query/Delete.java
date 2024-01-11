package Query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import QueryExceptions.InvalidConditionException;
import QueryExceptions.TableNotFoundException;

public class Delete {
    private String tablename;
    private String condition;

    public Delete(String tablename, String condition) {
        this.tablename = tablename;
        this.condition = condition;
    }

    public String execute() {
        try {
            if (FileHandling.checkexistance(tablename)) {
                try {
                    boolean deleteOccured = false;
                    List<String> lines = FileHandling.readFromFile(tablename);
                    List<String> updatedLines = new ArrayList<>();
                    String columnNamesString = lines.get(0);
                    if (lines.size() == 1) {
                        return"THERE IS NO RECORDS TO DELETE";
                    }
                    if (condition == null || condition.isEmpty()) {
                        if (!lines.isEmpty()) {
                            updatedLines.add(lines.get(0)); 
                        }
                        deleteOccured = true;
                    } else {
                        for (String line : lines) {

                            if (WhereCondition.checkWhereCondition(line, condition, columnNamesString)) {
                                deleteOccured = true;
                                continue;
                            } else {
                                updatedLines.add(line);
                            }
                        }
                    }
                    if (deleteOccured) {
                        FileHandling.writeToFile(tablename, updatedLines);
                        return "record deleted successfully." ;
                    } else {
                        throw new InvalidConditionException("INVALID CONDITION");
                    }

                } catch (

                IOException e) {
                    return "Error deleting table. ";
                    
                }
            } else {
                throw new TableNotFoundException(tablename);
            }
        } catch (TableNotFoundException e) {

           return e.getMessage();
        } catch (InvalidConditionException e) {
            return e.getMessage();
        }
    }
}
