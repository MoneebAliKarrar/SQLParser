package Query;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import QueryExceptions.TableAlreadyExists;
import QueryExceptions.TableNotFoundException;

public class FileHandling {
    private static final String BASE_PATH = "/Users/Admin/Desktop/MyDatabasesjava/";
    private static final String TABLE_FILE_EXTENSION = ".txt";

     public static String getTableFilePath(String tableName) {
        return BASE_PATH + File.separator + tableName + TABLE_FILE_EXTENSION;
    }

    public static void createTableFile(String tablename) throws TableAlreadyExists {
        File tablefile = new File(getTableFilePath(tablename));
        try{
        if (!checkexistance(tablename)) {
            try {
                if (tablefile.createNewFile()) {
                    System.out.println("Table's file is been created successfully");
                } else {
                    System.out.println("Table's file creation failed");
                }
            } catch (IOException e) {
                System.out.println("Table's file creation failed. Error: " + e.getMessage());
                e.printStackTrace();
            }
        } else {
            throw new TableAlreadyExists(tablename);
        }}catch(TableAlreadyExists e){
            System.out.println("Table Already Exists: " + e.getMessage());

        }

    }
    public static String dropTableFile(String tablename) throws TableNotFoundException {
        File tablefile = new File(getTableFilePath(tablename));
        try{
        if (checkexistance(tablename)) {
            if (tablefile.delete()) {
                return "Table's file is been dropped successfully";
            } else { 
                return "Table's file dropping failed";
            }
        } else {
            throw new TableNotFoundException(tablename);
        }}catch(TableNotFoundException e){
            return "Table not found: " + e.getMessage();
        }
    }

   public static List<String> readFromFile(String tablename) throws IOException{
    File tableFile = new File(getTableFilePath(tablename));
    try{
        if (checkexistance(tablename)) {
            try {
                List<String> lines = new ArrayList<>();
                try (BufferedReader reader = new BufferedReader(new FileReader(tableFile))) {
                    String line;
                    while ((line = reader.readLine()) != null) 
                        lines.add(line);
                    }
                    return lines;

            } catch (IOException e) {
                System.out.println("Error reading table. " + e.getMessage());
                e.printStackTrace();
            }
        } else {
            throw new TableNotFoundException(tablename);
        }}catch(TableNotFoundException e){
            System.out.println("Table not found: " + e.getMessage());
        }
        return new ArrayList<>();
        
   }
   public static void writeToFile(String tablename,List<String> values){
    File tableFile = new File(getTableFilePath(tablename));
    try{
        if (checkexistance(tablename)) {
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(tableFile))) {
                for (String value : values) {
                    writer.write(value);
                    writer.newLine();
                }
            } catch (IOException e) {
                System.out.println("Error inserting record." + e.getMessage());
                e.printStackTrace();
            }
        } else {
            throw new TableNotFoundException(tablename);
        }}catch(TableNotFoundException e){
            System.out.println("Table not found: " + e.getMessage());
        }
   }
   public static boolean checkexistance(String tablename){
    File tableFile = new File(getTableFilePath(tablename));
    if (tableFile.exists()) {
        return true;
    }else{
        return false;
    }
   }
   public static void writeColumnsToFile(String tablename, List<String> values) {
    File tableFile = new File(getTableFilePath(tablename));
    try{
        if (tableFile.exists()) {
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(tableFile, true))) {
                StringBuilder line = new StringBuilder();
                for (String value : values) {
                    line.append(value).append(",");
                }
                line.setLength(line.length() - 1);
                line.append("\n");
                writer.write(line.toString());
            } catch (IOException e) {
                System.out.println("Error inserting record." + e.getMessage());
                e.printStackTrace();
            }
        } else {
            throw new TableNotFoundException(tablename);
        }}catch(TableNotFoundException e){
            System.out.println("Table not found: " + e.getMessage());
        }

}

}


