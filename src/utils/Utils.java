package utils;

/**
 * Created by Axel on 05/10/2018.
 * All server calls for on demand label construction.
 * REST API
 */
public class Utils {

    public static String toNumberLong(String l){
        return "NumberLong("+l+")";
    }

    public static String toNumberLong(Long l){
        return "\"NumberLong("+l+")\"";
    }
}
