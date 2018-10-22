package utils;

import org.apache.commons.io.FileUtils;
import trikita.log.Log;

import java.io.*;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Axel on 05/10/2018.
 * Utils class
 */
public class Utils {

    private static String TAG = Utils.class.getName();

    static List<String> findAllFilesWithExt(String dirPath){
        List<String> paths = new ArrayList<>();
        try {
            File dir = new File(dirPath);
            String[] extensions = new String[] { "xlsx", "csv" };
            Log.d(TAG,"Getting all .xlsx and .csv files in " + dir.getCanonicalPath()
                    + " including those in subdirectories");
            List<File> files = (List<File>) FileUtils.listFiles(dir, extensions, true);
            for (File file : files) {
                if(file.getCanonicalPath().contains("~")) continue;
                paths.add(file.getCanonicalPath());
                Log.d(TAG,"file: " + file.getCanonicalPath());
            }
        } catch (IOException | IllegalArgumentException e) {
            Log.e(TAG,"error while getting file paths in directory", e);
        }
        return paths;
    }

    static String humanReadableFormat(Duration duration) {
        return duration.toString()
                .substring(2)
                .replaceAll("(\\d[HMS])(?!$)", "$1 ")
                .toLowerCase();
    }

    static boolean isNumeric(String str)
    {
        try
        {
            double d = Double.parseDouble(str);
        }
        catch(NumberFormatException nfe)
        {
            return false;
        }
        return true;
    }

    public static void writeToLogFile(String input){
        PrintWriter out = null;
        try {
            out = new PrintWriter(new BufferedWriter(new FileWriter("log.txt", true)));
            out.println(input);
        }catch (IOException e) {
            System.err.println(e);
        }finally{
            if(out != null){
                out.close();
            }
        }

    }
}
