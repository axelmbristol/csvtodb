import database.DataBase;
import entities.CSVTagData;
import trikita.log.Log;

public class Main {

    private static String TAG = Main.class.getName();

    public static void main(String[] args) {
        Log.d(TAG,"start...");
        DataBase dataBase = new DataBase("main");
        dataBase.addData(new CSVTagData("20/01/2015 00:00", "6:00:00 AM",
                70101100019L, 40101310285L, 12,
                "-75@", "BB","",0,
                "","-3:3:-21:-14:25:29",
                "I",6
                ));
    }

}
