import org.apache.commons.text.StringSubstitutor;
import org.duckdb.DuckDBConnection;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class CreateParquet {

    private static final String CREATE_BASE_TEMPLATE = "CREATE TABLE ${name} (${cols})";
    private static final Pattern CSV_FILE_NAME_REGEX = Pattern.compile("covid_data_(\\d+)_(\\d+)\\..+");

    public static void main(String[] args) throws SQLException, IOException {
        if(args.length == 2) {
            String csvFile = args[0];
            String parquetPath = args[1];
            createParquetFile(csvFile, parquetPath);
        } else if (args.length == 1) {
            convertFolder(args[0]);
        }
    }

    private static void convertFolder(String folderPath) throws SQLException, IOException {
        File csvFolder = new File(folderPath);
        if(!csvFolder.isDirectory()) {
            throw new IllegalArgumentException("Input is not a folder - " + folderPath);
        }

        String[] csvFiles = csvFolder.list((parent, filename) -> filename.endsWith(".csv"));
        for (String csvFile : csvFiles) {
            String parquetFilename = csvFile.substring(0, csvFile.lastIndexOf('.')) + ".parquet";
            System.out.println("Converting " + csvFile + " to " + parquetFilename);
            createParquetFile(new File(csvFolder, csvFile).getAbsolutePath(), new File(csvFolder, parquetFilename).getAbsolutePath());
        }
    }

    private static void createParquetFile(String csvFile, String parquetPath) throws SQLException, IOException {
        try(DuckDBConnection duckDBConnection = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:")) {
            List<TestLoadTimes.DbCol> dbCols = createDuckDBTable(duckDBConnection, getColCount(csvFile));
            String colName = dbCols.stream().map(TestLoadTimes.DbCol::getParquetColName).collect(Collectors.joining(", "));
            loadDuckDBTable(colName, csvFile, duckDBConnection);
            exportToParquet(parquetPath, duckDBConnection);
        }
    }

    private static void exportToParquet(String parquetPath, DuckDBConnection duckDBConnection) throws SQLException {
        try(Connection connection = duckDBConnection.duplicate()) {
            try(Statement stmt = connection.createStatement()) {
                stmt.execute("COPY (SELECT * FROM " + TestLoadTimes.TABLE_NAME + ") TO '" + parquetPath + "' (FORMAT 'parquet')");
            }
        }
    }

    private static void loadDuckDBTable(String colNames, String csvFile, DuckDBConnection connection) throws SQLException {
        String sql = String.format("COPY %s(%s) FROM '%s' (DELIMITER ',', HEADER, COMPRESSION gzip)",
                TestLoadTimes.TABLE_NAME, colNames, csvFile);
        TestLoadTimes.loadCsv(sql, connection);
    }

    public static int getColCount(String csvFile) {
        Matcher matcher = CSV_FILE_NAME_REGEX.matcher(new File(csvFile).getName());
        if(matcher.matches()) {
            return Integer.parseInt(matcher.group(1));
        }
        return -1;
    }

    private static List<TestLoadTimes.DbCol> createDuckDBTable(DuckDBConnection duckDBConnection, int colCount) throws SQLException, IOException {
        return TestLoadTimes.createTable(duckDBConnection, CreateParquet::createDuckDBTable, colCount);
    }

    public static void createDuckDBTable(List<TestLoadTimes.DbCol> tableCols, Statement stmt) {
        String tableColDefinitions = tableCols.stream().map(TestLoadTimes.DbCol::getParquetColDefn).collect(Collectors.joining(", "));

        Map<String, String> vars = new HashMap<>();
        vars.put("name", TestLoadTimes.TABLE_NAME);
        vars.put("cols", tableColDefinitions);

        String createTable = StringSubstitutor.replace(CREATE_BASE_TEMPLATE, vars);

        try {
            stmt.execute(createTable);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
