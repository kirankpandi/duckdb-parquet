import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.text.StringSubstitutor;
import org.duckdb.DuckDBConnection;

import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

public class TestLoadTimes {

    private static final String CREATE_SEQUENCE_TEMPLATE = "CREATE SEQUENCE ${sequence} START 0 MINVALUE 0";
    private static final String CREATE_BASE_TEMPLATE = "CREATE TABLE ${name} (id INTEGER NOT NULL DEFAULT " +
            "(nextval('${sequence}')), ${cols}, PRIMARY KEY (id))";
    private static final String INGEST_PARQUET_TEMPLATE = "CREATE TABLE ${name} AS SELECT file_row_number AS id," +
            " ${cols} FROM read_parquet('${pqt_file}', file_row_number=true)";
    private static final String ADD_UNIQUE_CONSTRAINT = "CREATE UNIQUE INDEX TtempTable_idx ON TtempTable(id)";

    public static String TABLE_NAME = "TtempTable";
    public static class DbCol {
        public String name;
        public String type;
        public int index;

        public String getCsvTableColDefn() {
            return "A" + index + " " + type;
        }

        public String getCsvColName() {
            return "A" + index;
        }

        public String getParquetColDefn() {
            return name + " " + type;
        }

        public String getParquetColName() {
            return name;
        }

        public String getParquetIngestColumnDefn() {
            return name + " AS " + getCsvColName();
        }
    }

    public static void main(String[] args) throws IOException, SQLException {
        if(args.length == 0) {
            System.out.println("Input file path not specified");
            return;
        }

        String fileName = args[0];

        File inputArg = new File(args[0]);

        if(!inputArg.exists()) {
            throw new IllegalArgumentException("Input file doesn;t exist");
        }

        if(inputArg.isFile()) {
            measureLoadTime(inputArg.getAbsolutePath());
        } else if(inputArg.isDirectory()){
            File[] inputFiles = inputArg.listFiles((parent, name) -> name.endsWith(".csv") || name.endsWith(".parquet"));
            for (File inputFile :
                    inputFiles) {
                System.out.println("Calculating load for : " + inputFile);
                measureLoadTime(inputFile.getAbsolutePath());
            }
        }
    }

    private static void measureLoadTime(String fileName) throws SQLException, IOException {
        try(DuckDBConnection connection = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:")) {
            if (fileName.endsWith(".csv")) {
                loadCsvFile(fileName, connection);
            } else {
                loadParquetFile(fileName, connection);
            }
            printDbSize(connection);
        }
    }

    private static void printDbSize(DuckDBConnection connection) throws SQLException {
        try(Statement stmt = connection.createStatement()) {
            try(ResultSet rs = stmt.executeQuery("pragma database_size")) {
                if(rs.next()) {
                    System.out.println("Database size = " + rs.getString("memory_usage"));
                } else {
                    System.out.println("Unable to get database size");
                }
            }
        }
    }

    // uid,location_type,fips_code,location_name,state,date,total_population,cumulative_cases,
    // cumulative_cases_per_100_000,cumulative_deaths,cumulative_deaths_per_100_000,new_cases,
    // new_deaths,new_cases_per_100_000,new_deaths_per_100_000,new_cases_7_day_rolling_avg,new_deaths_7_day_rolling_avg

    private static void loadParquetFile(String parquetFile, DuckDBConnection duckDBConnection) throws IOException, SQLException {
        ObjectMapper objectMapper = new ObjectMapper();
        List<DbCol> tableCols = objectMapper.readValue(TestLoadTimes.class.getResourceAsStream("schema.json"), new TypeReference<>() {
        });
        tableCols = generateCols(tableCols, CreateParquet.getColCount(parquetFile));
        String tableColDefinitions = tableCols.stream().map(DbCol::getParquetIngestColumnDefn).collect(Collectors.joining(", "));

        Map<String, String> vars = new HashMap<>();
        vars.put("name", TABLE_NAME);
        vars.put("pqt_file", parquetFile);
        vars.put("cols", tableColDefinitions);

        String createTable = StringSubstitutor.replace(INGEST_PARQUET_TEMPLATE, vars);

        Instant start = Instant.now();
        try(Connection conn = duckDBConnection.duplicate()) {
            try(Statement stmt = conn.createStatement()) {
                stmt.execute(createTable);
                stmt.execute(ADD_UNIQUE_CONSTRAINT);
            }
        }
        System.out.println("Time ot load (ms): " + Duration.between(start, Instant.now()).toMillis());
//        printDbSize(duckDBConnection);
        start = Instant.now();
        doSelect(duckDBConnection);
        System.out.println("Time to select (ms) : " + Duration.between(start, Instant.now()).toMillis());
    }

    private static void loadCsvFile(String csvFile, DuckDBConnection connection) throws IOException, SQLException {
        List<DbCol> tableCols = createTable(connection, TestLoadTimes::createDuckDBTable, CreateParquet.getColCount(csvFile));
        String sql = String.format("COPY %s(%s) FROM '%s' (DELIMITER ',', HEADER, COMPRESSION gzip)",
                TABLE_NAME, tableCols.stream().map(DbCol::getCsvColName).collect(Collectors.joining(", ")), csvFile);
        Instant start = Instant.now();
        loadCsv(sql, connection);
        System.out.println("Time ot load (ms): " + Duration.between(start, Instant.now()).toMillis());
        start = Instant.now();
        doSelect(connection);
        System.out.println("Time to select (ms) : " + Duration.between(start, Instant.now()).toMillis());
    }

    private static void doSelect(DuckDBConnection connection) throws SQLException {
        try(Connection connection1 = connection.duplicate()) {
            try(Statement stmt = connection1.createStatement()) {
                int rowCount = 0;
                try(ResultSet resultSet = stmt.executeQuery("select count(*) from " + TABLE_NAME)) {
                    if (resultSet.next()) {
                        rowCount = resultSet.getInt(1);
                        System.out.println("Row count = " + rowCount);
                    }
                }
                try(ResultSet resultSet = stmt.executeQuery("select * from " + TABLE_NAME + " where id > "
                        + (rowCount / 2) + " and id <= " + ((rowCount / 2) + 5))) {
                    while (resultSet.next()) {
                        printRow(resultSet);
                    }
                }
            }
        }
    }

    private static void printRow(ResultSet resultSet) throws SQLException {
        int columnCount = resultSet.getMetaData().getColumnCount();
        for (int i = 1; i <= columnCount; i++) {
            resultSet.getString(i);
//            System.out.print(resultSet.getString(i) + (i < columnCount ? ", " : ""));
        }
//        System.out.println();
    }

    public static void loadCsv(String sql, DuckDBConnection duckDBConnection) throws SQLException {
        try(Connection connection = duckDBConnection.duplicate()) {
            try(Statement statement = connection.createStatement()) {
                statement.execute(sql);
            }
        }
    }

    public static List<DbCol> createTable(DuckDBConnection connection, BiConsumer<List<DbCol>, Statement> createTableConsumer, int colCount) throws IOException, SQLException {
        ObjectMapper objectMapper = new ObjectMapper();
        List<DbCol> tableCols = objectMapper.readValue(TestLoadTimes.class.getResourceAsStream("schema.json"), new TypeReference<>() {
        });
        if(colCount != -1) {
            tableCols = generateCols(tableCols, colCount);
        }
//        String tableColDefinitions = tableCols.stream().map(coldDefnMapper).collect(Collectors.joining(", "));

//        Map<String, String> vars = new HashMap<>();
//        vars.put("name", TABLE_NAME);
//        vars.put("sequence", "idvalues_" + TABLE_NAME);
//        vars.put("cols", tableColDefinitions);
//
//        String createSequence = StringSubstitutor.replace(CREATE_SEQUENCE_TEMPLATE, vars);
//        String createTable = StringSubstitutor.replace(CREATE_BASE_TEMPLATE, vars);

        try(Connection dbConnection = connection.duplicate()) {
            try(Statement stmt = dbConnection.createStatement()) {
//                stmt.execute(createSequence);
//                stmt.execute(createTable);
                createTableConsumer.accept(tableCols, stmt);
            }
        }

        return tableCols;
    }

    public static List<DbCol> generateCols(List<DbCol> tableCols, int colCount) {
        int inputSize = tableCols.size();
        int q = colCount / inputSize;
        List<DbCol> opCols = new ArrayList<>(colCount);
        for(int i = 0; i < q; i++) {
            for(int j = 0; j < tableCols.size(); j++) {
                DbCol newCol = new DbCol();
                newCol.type = tableCols.get(j).type;
                newCol.index = i * inputSize + j;
                newCol.name = tableCols.get(j).name + "_" + i;
                opCols.add(newCol);
            }
        }
        int r = colCount % inputSize;
        for (int i = 0; i < r; i++) {
            DbCol newCol = new DbCol();
            newCol.type = tableCols.get(i).type;
            newCol.index = q * inputSize + i;
            newCol.name = tableCols.get(i).name + "_" + q;
            opCols.add(newCol);
        }
        return opCols;
    }

    public static void createDuckDBTable(List<DbCol> tableCols, Statement stmt) {
        String tableColDefinitions = tableCols.stream().map(DbCol::getCsvTableColDefn).collect(Collectors.joining(", "));

        Map<String, String> vars = new HashMap<>();
        vars.put("name", TABLE_NAME);
        vars.put("sequence", "idvalues_" + TABLE_NAME);
        vars.put("cols", tableColDefinitions);

        String createSequence = StringSubstitutor.replace(CREATE_SEQUENCE_TEMPLATE, vars);
        String createTable = StringSubstitutor.replace(CREATE_BASE_TEMPLATE, vars);

        try {
            stmt.execute(createSequence);
            stmt.execute(createTable);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
