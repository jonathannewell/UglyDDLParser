package com.vmware.file.sql;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

@Data
@Slf4j
public class DDLFileReader {

    public static String DELIMITER = "_";

    private File file;
    private String filename;
    private String tableName;
    private String database;
    private String schema;
    private Index index;
    private List<String> schemas = List.of("dbo", "rae", "subm");
    private List<Column> columns = new ArrayList<>();
    private int recordSize;

    public DDLFileReader(File file) {
        this.file = file;
        this.filename = file.getName();
        if (!file.exists()) throw new IllegalArgumentException("File [" + filename + "] does not exist!");
        parseFileName();
    }

    private void parseFileName() {
        if (!filename.contains(DELIMITER))
            throw new IllegalArgumentException("File [" + filename + "] does not meet naming convention requirements!");
        try {
            int pointer1 = filename.indexOf(DELIMITER);
            this.index = findSchema(pointer1);
            this.database = filename.substring(0, index.first - 1);
            index.first = index.second + 1;
            index.second = filename.indexOf(".txt", index.first);
            this.tableName = filename.substring(index.first, index.second);
        } catch (Throwable t) {
            log.error("Failed Parsing Filename [{}]! Details: {}", filename, t.getMessage());
            throw t;
        }
    }

    public void parse() throws IOException {

        AtomicBoolean shouldParseColumn = new AtomicBoolean(false);
        AtomicBoolean isView = new AtomicBoolean(false);

        if(!index.schemaFound) {
            log.error("Unable to Parse File [{}]. Schema was not found and therefore Create Table Statement cannot be validated...", filename);
            return;
        }

        Files.lines(file.toPath()).forEach(line -> {
            if (shouldParseColumn.get()) {
                Column c = new Column(line);
                if (c.parse()) {
                    columns.add(c);
                } else {
                    shouldParseColumn.set(false);
                }
            } else {
                if (line.contains("CREATE")) {
                    if (!line.contains("VIEW")) {
                        Pattern p = Pattern.compile("CREATE TABLE.*\\[" + schema + "\\].\\[" + tableName + "\\].*\\(", Pattern.CASE_INSENSITIVE);
                        if (p.matcher(line).matches()) {
                            shouldParseColumn.set(true);
                        }
                    }else {
                        isView.set(true);
                    }
                }
            }
        });

        columns.forEach(c -> recordSize += c.bytesMax());
        if (recordSize == 0 && !isView.get()) {
            log.warn("Did not correctly determine record size! Uh Oh!");
        }
    }

    public String getInfo(){
        return String.format("%s,%s,%s,%d,%s,%d", database, schema, tableName, recordSize, hasDateColumns(), columns.size());
    }

    private Index findSchema(int pointer) {
        Index i = new Index();
        try {
            i.first = pointer + 1;
            i.second = filename.indexOf(DELIMITER, i.first);
            if (i.first > filename.length() || i.second == -1){
                log.error("Schema Not Found in Filename [{}]...is this schema in the valid schema list???", filename);
                return i;
            }
            this.schema = filename.substring(i.first, i.second);
            if (schemas.contains(schema.toLowerCase())) {
                i.schemaFound = true;
                return i;
            }
            return findSchema(i.second);
        }
        catch(Throwable t){
            log.error("Error Finding Schema! Details: {}", t.getMessage());
            return i;
        }
    }

    public boolean hasDateColumns(){
        for(Column c:columns){
            if (c.name.equalsIgnoreCase("CreatedOn") ||
                c.name.equalsIgnoreCase("EditedOn"))  {
               return true;
            }
        }

        return false;
    }

    public boolean isSoftDelete(){
        for(Column c:columns){
            if (c.name.equalsIgnoreCase("Active") ||
                c.name.equalsIgnoreCase("Soft_Delete_Flag") ||
                c.name.equalsIgnoreCase("ActiveFlag")
            )  {
                return true;
            }
        }

        return false;
    }

    @Data
    private static class Index {
        private int first;
        private int second;
        private boolean schemaFound;
    }

    @Data
    @Slf4j
    private static class Column {
        private String name;
        private String dataType;
        private int size;
        private String line;

        Column(String line) {
            this.line = line;
        }

        private boolean parse() {
            if (line.stripLeading().charAt(0) != '[') return false;
            int firstBracket = line.indexOf("[");
            int pointer = line.indexOf("]", firstBracket);
            try {

                name = line.substring(firstBracket + 1, pointer);
                pointer = line.indexOf("[", pointer + 1);
                pointer += 1;
                int pointer2 = line.indexOf("]", pointer);
                dataType = line.substring(pointer, pointer2);

                pointer = pointer2 + 1;
                pointer2 = line.indexOf("(", pointer);
                if (pointer2 > 0) {
                    String sizeData = line.substring(pointer2 + 1, line.indexOf(")", pointer2));
                    try {
                        size = Integer.parseInt(sizeData);
                    } catch (NumberFormatException nfe) {
                        try {
                            if (sizeData.equals("max")) {
                                size = 8000;
                            } else {
                                size = Integer.parseInt(sizeData.split(",")[0]);
                            }
                        } catch (NumberFormatException nfe2) {
                            log.error("Issue parsing size for column[{}] type[{}]! Found [{}] Details: {}", name, dataType, sizeData, nfe2.getMessage());
                            size = 1;
                        }
                    }
                } else {
                    size = 1;
                }

                log.debug("Found Column named [{}] Type[{}] Size[{}]", name, dataType, size);
                return true;
            } catch (Throwable t) {
                log.error("Error Parsing Line \"{}\" 1st[: {} 1st]: {} Details: {}", line, firstBracket, pointer, t.getMessage());
                return false;
            }
        }

        private int bytesMax() {
            switch (dataType.toLowerCase()) {
                case "numeric":
                case "decimal":
                    if (size > 0 && size < 10) return 5;
                    if (size > 9 && size < 20) return 9;
                    if (size > 19 && size < 29) return 13;
                    if (size > 28 && size < 39) return 17;
                    throw new IllegalStateException("Invalid Precision for Decimal or Numeric! Data Type [" + dataType + "] for Column [" + name + "] Size [" + size + "]");
                case "float":
                    if (size > 0 && size < 25) return 4;
                    if (size > 24 && size < 54) return 8;
                    throw new IllegalStateException("Invalid Precision for Float! Data Type [" + dataType + "] for Column [" + name + "] Size [" + size + "]");
                case "date":
                    return 3;
                case "int":
                case "smalldatetime":
                    return 4;
                case "bigint":
                case "datetime":
                    return 8;
                case "smallint":
                    return 2;
                case "tinyint":
                case "bit":
                    return 1;
                case "varchar":
                case "char":
                case "binary":
                    return size;
                case "nvarchar":
                    return size * 2;
                case "uniqueidentifier":
                    return 16;
                default:
                    throw new IllegalStateException("Unknown Data Type [" + dataType + "] for Column [" + name + "]");
            }
        }
    }
}
