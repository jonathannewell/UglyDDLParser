package com.vmware.file.sql;


import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;

@SpringBootApplication
@Slf4j
public class Runner implements CommandLineRunner {

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
        AtomicInteger noCnt = new AtomicInteger();
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
                        noCnt.getAndIncrement();
                        noDateFiles.println(ddlFile.getInfo());
                    }
                } catch (Throwable t) {
                    log.error("Error parsing file [{}] Details: {}", path.getFileName(), t.getMessage());
                }
            }
        });
        writer.close();
        noDateFiles.close();
        log.info("Found [{}] Total SQL Files and [{}] do not have date fields", cnt.get(), noCnt.get());
    }
}
