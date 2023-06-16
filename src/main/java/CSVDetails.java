import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class CSVDetails {

    public static void main(String[] args) throws IOException {
        if(args.length < 1) {
            System.out.println("Usage");
            return;
        }

        BufferedReader rdr = new BufferedReader(new FileReader(args[0]));
        String[] cols = rdr.readLine().split(",");
        System.out.println("Col count = " + cols.length);
    }
}
