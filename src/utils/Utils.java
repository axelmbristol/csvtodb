package utils;

import org.apache.commons.io.FileUtils;
import trikita.log.Log;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Axel on 05/10/2018.
 * Utils class
 */
public class Utils {

    private static String TAG = Utils.class.getName();

    public static List<String> findAllFilesWithExt(String dirPath){
        List<String> paths = new ArrayList<>();
        try {
            File dir = new File(dirPath);
            String[] extensions = new String[] { "xlsx", "csv" };
            Log.d(TAG,"Getting all .xlsx and .csv files in " + dir.getCanonicalPath()
                    + " including those in subdirectories");
            List<File> files = (List<File>) FileUtils.listFiles(dir, extensions, true);
            for (File file : files) {
                Log.d(TAG,"file: " + file.getCanonicalPath());
                paths.add(file.getCanonicalPath());
            }
        } catch (IOException e) {
            Log.e(TAG,"error while getting file paths im directory", e);
        }
        return paths;
    }

    public static String humanReadableFormat(Duration duration) {
        return duration.toString()
                .substring(2)
                .replaceAll("(\\d[HMS])(?!$)", "$1 ")
                .toLowerCase();
    }
}
