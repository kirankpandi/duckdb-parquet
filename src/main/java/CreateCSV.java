import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPOutputStream;

public class CreateCSV {

    private static final int INPUT_FILE = 0;
    private static final int OUTPUT_FILE = 1;
    private static final int LINE_COUNT = 2;
    private static final int COL_COUNT = 3;

    private static final int ARG_COUNT = 4;

    public static void main(String[] args) throws IOException {

        if(args.length < 4) {
            return;
        }

        int lineCount = Integer.parseInt(args[LINE_COUNT]);
        int colCount = Integer.parseInt(args[COL_COUNT]);
        GZIPOutputStream opFile = new GZIPOutputStream(new FileOutputStream(args[OUTPUT_FILE]));
//        FileOutputStream opFile = new FileOutputStream(args[OUTPUT_FILE]);

        try(opFile;
            BufferedReader rdr = new BufferedReader(new FileReader(args[INPUT_FILE], StandardCharsets.UTF_8))) {
            String inputLine;
            int count = 0;
            while ((inputLine = rdr.readLine()) != null && count <= lineCount) {
                opFile.write((addColumns(inputLine, colCount) + "\n").getBytes(StandardCharsets.UTF_8));
                count++;
            }
            opFile.flush();
        }

    }

    private static String addColumns(String inputLine, int colCount) {
        List<String> cols = new ArrayList<>(colCount);
        String[] inputCols = inputLine.split(",", 20);
        int q = colCount / inputCols.length;
        for(int i = 0; i < q; i++) {
            cols.addAll(List.of(inputCols));
        }
        q = colCount % inputCols.length;
        for(int i = 0; i < q; i++) {
            cols.add(inputCols[i]);
        }
        return String.join(",", cols);
    }
}
