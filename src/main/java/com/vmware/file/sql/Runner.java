package com.vmware.file.sql;


import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;

@SpringBootApplication
@Slf4j
public class Runner implements CommandLineRunner {

    private static int LARGE_ROW_SIZE = 8000;
    private static int HIGH_COLUMN_CNT = 20;

    public static void main(String... args) {

        log.info("Starting SQL File Parsing Run...");
        SpringApplication.run(Runner.class, args);
        log.info("Run Complete!!!!!");

    }

    @Override
    public void run(String... args) throws Exception {
        if (args.length == 0) {
            System.err.println("Please provide the directory path where the DDL files can be found as a program argument!");
            return;
        }

        log.info("Looking for DDL Files in [{}]", args[0]);
        AtomicInteger cnt = new AtomicInteger();
        AtomicInteger cntLargeRowSize = new AtomicInteger();
        AtomicInteger cntWithOutDateFields = new AtomicInteger();
        PrintWriter writer = new PrintWriter(new FileWriter("Table-sizes.csv"));
        PrintWriter noDateFiles = new PrintWriter(new FileWriter("Table-With-No-Date-Fields.csv"));
        Files.list(Path.of(args[0])).forEach(path -> {
            File f = path.toFile();
            if (!f.isDirectory()) {
                try {
                    DDLFileReader ddlFile = new DDLFileReader(path.toFile());
                    log.debug("File [{}] -> DB[{}] Schema[{}] Table[{}]", ddlFile.getFilename(), ddlFile.getDatabase(), ddlFile.getSchema(), ddlFile.getTableName());
                    cnt.getAndIncrement();

                    ddlFile.parse();
                    writer.println(ddlFile.getInfo());
                    if (!ddlFile.hasDateColumns()) {
                        cntWithOutDateFields.getAndIncrement();
                        noDateFiles.println(ddlFile.getInfo());
                    }
                    if (ddlFile.getRecordSize() > LARGE_ROW_SIZE || ddlFile.getColumns().size() > HIGH_COLUMN_CNT){
                        cntLargeRowSize.getAndIncrement();
                    }
                } catch (Throwable t) {
                    log.error("Error parsing file [{}] Details: {}", path.getFileName(), t.getMessage());
                }
            }
        });
        writer.close();
        noDateFiles.close();

        int withDateFields = cnt.get() - cntWithOutDateFields.get();

        BigDecimal total = new BigDecimal(cnt.get());
        BigDecimal oneHundred = new BigDecimal(100);
        BigDecimal pctWithDate = new BigDecimal(withDateFields).divide(total,2,RoundingMode.HALF_DOWN).multiply(oneHundred).setScale(0);
        BigDecimal pctWithoutDate = new BigDecimal(cntWithOutDateFields.get()).divide(total, 2,RoundingMode.HALF_DOWN).multiply(oneHundred).setScale(0);
        BigDecimal pctLargeRow = new BigDecimal(cntLargeRowSize.get()).divide(total, 2,RoundingMode.HALF_DOWN).multiply(oneHundred).setScale(0);
        log.info("Found [{}] Total SQL Files. [{} or {}%] with date fields, [{} or {}%] without date fields, [{} or {}%] with large row sizes or high column counts", cnt.get(), withDateFields, pctWithDate, cntWithOutDateFields.get(),pctWithoutDate, cntLargeRowSize.get(),pctLargeRow);
    }
}
