package Query;


import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import QueryExceptions.TableNotFoundException;


public class Insert {
    private String tablename;
    public Insert(String tablename){
        this.tablename = tablename;
    }
    public String execute(Object[] values){
        try {
        if (FileHandling.checkexistance(tablename)) {
            List<String> stringValues = Arrays.stream(values)
                        .map(String::valueOf)
                        .collect(Collectors.toList());
                FileHandling.writeColumnsToFile(tablename, stringValues);
                return "Record inserted successfully";
            
        } else {
            throw new TableNotFoundException(tablename);
        }}
        catch(TableNotFoundException e){
            return "Table not found: " + e.getMessage();
        }
    }
}
