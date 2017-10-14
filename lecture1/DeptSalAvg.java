import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class DeptSalAvg {
    public static void main(String[] args){
        try {
            FileReader file = new FileReader("C:\\Users\\teacher\\Desktop\\emp_salary.dat");
            BufferedReader br = new BufferedReader(file);
            String record;
            String[] fields;
            while((record = br.readLine()) != null){
                System.out.println(record);
                fields = record.split("\\|");
                System.out.println("key: " + fields[0] + " , value: " + fields[2]);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
