import trikita.log.Log;
import utils.XLSXParser;

public class Main {

    private static String TAG = Main.class.getName();

    public static void main(String[] args) {
        Log.d(TAG,"start...");
        XLSXParser.init("C:\\Users\\Axel\\Desktop\\data","main");
    }

}
