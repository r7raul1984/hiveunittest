package com.jj.test;

import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.StandaloneHiveRunner;
import com.klarna.hiverunner.annotations.HiveSQL;
import com.klarna.hiverunner.data.TsvFileParser;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;

import java.io.File;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/*
    This example is intended to be a small show case for some of the ways of setting up your test data in HiveRunner.
    It will only print out some result and thus is not a strict unit test suite.

    The examples will go through cases with adding test data from "code" or from file, and how you only need to supply
    a selected subset of the columns or how to use more advanced features like files with custom separator characters
    or custom NULL keywords in the test data files.
 */
@RunWith(StandaloneHiveRunner.class)
public class InsertTestData {
    @Rule
    public TestName name = new TestName();

    @HiveSQL(files = {})
    private HiveShell shell;

    @Before
    public void setupDatabase() {
        shell.execute("CREATE DATABASE source_db");
        shell.execute(new StringBuilder()
                .append("CREATE TABLE source_db.test_table (")
                .append("col_a STRING, col_b INT, col_c BOOLEAN")
                .append(")")
                .toString());
        shell.execute(new StringBuilder()
                .append("CREATE TABLE `source_db.test_data`(")
                .append(" `test_version` string, \n" +
                        "  `id` string \n"
                )
                .append(")PARTITIONED BY ( \n" +
                        "  `date` string, \n" +
                        "  `hour` string)\n" +
                        "  stored as orc  ")
                .toString());
        shell.execute(new StringBuilder()
                .append("  CREATE TABLE `source_db.test_log`(")
                .append(" `test_version` string, \n" +
                        " `exp_list` string, \n" +
                        "  `id` string \n"
                )
                .append(")" +
                        "  stored as orc  ")
                .toString());

    }

    @Test
    public void insertRowsFromCsvFile() {
        File dataFile = new File("src/test/resources/examples/test_data.csv");
        shell.insertInto("source_db", "test_data")
                .withAllColumns()
                .addRowsFrom(dataFile, new TsvFileParser().withHeader().withDelimiter(",").withNullValue("NULL"))
                .commit();

        File logFile = new File("src/test/resources/examples/test_log.csv");
        shell.insertInto("source_db", "test_log")
                .withAllColumns()
                .addRowsFrom(logFile, new TsvFileParser().withHeader().withDelimiter(",").withNullValue("NULL"))
                .commit();

        List<Object[]> result = shell.executeStatement("select * from source_db.test_data a " +
                "left join ( select test_version,id,exp_list from source_db.test_log group by test_version,id,exp_list) b on a.id=b.id and a.test_version =b.test_version");
        printResult(result);
        assertEquals(2, result.size());

        assertArrayEquals(new Object[]{null, null, "2018-11-19", "00", null, null, null}, result.get(0));
        assertArrayEquals(new Object[]{"1_3", "jj3fa4e5c0f24", "2018-11-19", "00", "1_3", "jj3fa4e5c0f24", "1:1;2:6;3:8"}, result.get(1));
    }

    @Test
    public void insertRowsFromCode() {
        shell.insertInto("source_db", "test_table")
                .withAllColumns()
                .addRow("Value1", 1, true)
                .addRow("Value2", 99, false)
                .commit();

        printResult(shell.executeStatement("select * from source_db.test_table"));
    }


    @Test
    public void insertRowsFromCodeWithSelectedColumns() {
        shell.insertInto("source_db", "test_table")
                .withColumns("col_a", "col_c")
                .addRow("Value1", true)
                .addRow("Value2", false)
                .commit();

        printResult(shell.executeStatement("select * from source_db.test_table"));
    }


    @Test
    public void insertRowsFromTsvFile() {
        File dataFile = new File("src/test/resources/examples/data1.tsv");
        shell.insertInto("source_db", "test_table")
                .withAllColumns()
                .addRowsFromTsv(dataFile)
                .commit();

        printResult(shell.executeStatement("select * from source_db.test_table"));
    }


    @Test
    public void insertRowsFromTsvFileWithHeader() {
        File dataFile = new File("src/test/resources/examples/dataWithHeader1.tsv");
        TsvFileParser parser = new TsvFileParser().withHeader();
        shell.insertInto("source_db", "test_table")
                .addRowsFrom(dataFile, parser)
                .commit();

        printResult(shell.executeStatement("select * from source_db.test_table"));
    }

    @Test
    public void insertRowsFromTsvFileWithSubsetHeader() {
        File dataFile = new File("src/test/resources/examples/dataWithHeader2.tsv");
        TsvFileParser parser = new TsvFileParser().withHeader();
        shell.insertInto("source_db", "test_table")
                .addRowsFrom(dataFile, parser)
                .commit();

        printResult(shell.executeStatement("select * from source_db.test_table"));
    }


    @Test
    public void insertRowsIntoPartitionedTableStoredAsSequencefileWithCustomDelimiterAndNullValue() {
        File dataFile = new File("src/test/resources/examples/data2.tsv");
        shell.execute(new StringBuilder()
                .append("CREATE TABLE source_db.test_table2 (")
                .append("col_a STRING, col_b INT")
                .append(")")
                .append("partitioned by (col_c string)")
                .append("stored as SEQUENCEFILE")
                .toString());

        shell.insertInto("source_db", "test_table2")
                .withAllColumns()
                .addRowsFrom(dataFile, new TsvFileParser().withDelimiter(":").withNullValue("__NULL__"))
                .commit();

        printResult(shell.executeStatement("select * from source_db.test_table2"));
    }


    public void printResult(List<Object[]> result) {
        System.out.println(String.format("Result from %s:", name.getMethodName()));
        for (Object[] row : result) {
            System.out.println(Arrays.asList(row));
        }
    }
}