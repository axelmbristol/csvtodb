package utils;

import org.apache.commons.io.FileUtils;
import trikita.log.Log;

import java.io.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.LocalTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Axel on 05/10/2018.
 * Utils class
 */
public class Utils {

    private static String TAG = Utils.class.getName();

    public static boolean isTimeValid(String input){
        Pattern pattern = Pattern.compile("(1[012]|[1-9]):[0-5][0-9]:[0-5][0-9] ?(?i)(am|pm)");
        Matcher matcher = pattern.matcher(input);
        boolean isValid = matcher.find();
        //Log.d(TAG,"is "+input+" valid ? "+isValid);
        return isValid;
    }

    static List<String> findAllFilesWithExt(String dirPath){
        List<String> paths = new ArrayList<>();
        try {
            File dir = new File(dirPath);
            String[] extensions = new String[] { "xlsx" };
            Log.d(TAG,"Getting all .xlsx files in " + dir.getCanonicalPath()
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

    public static String prettyDate(Date date){
        return new SimpleDateFormat("dd/MM/yy").format(date);
    }

    public static Date dateFromString(String string){
        DateFormat format = new SimpleDateFormat("dd/MM/yy");
        Date date = null;
        try {
            date = format.parse(string);
        } catch (ParseException e) {
            Log.e(TAG,"error while getting date", e);
        }
        return date;
    }

    static String humanReadableFormat(Duration duration) {
        return duration.toString()
                .substring(2)
                .replaceAll("(\\d[HMS])(?!$)", "$1 ")
                .toLowerCase();
    }

    public static boolean isNumeric(String str)
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
