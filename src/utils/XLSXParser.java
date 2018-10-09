package utils;

import entities.CSVTagData;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import trikita.log.Log;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static utils.Utils.findAllFilesWithExt;
import static utils.Utils.humanReadableFormat;

/**
 * Created by Axel on 26/09/2018.
 */
public class XLSXParser {

    private static String TAG = XLSXParser.class.getName();

    public static void init(String dirPath){
        List<String> files = findAllFilesWithExt(dirPath);
        for (String path: files) {
            parse(path);
        }
    }

    private static List<CSVTagData> parse(String filePath){
        int i = 0;
        List<CSVTagData> result = new ArrayList<>();
        Instant start = Instant.now();
        Instant start2 = Instant.now();
        try {
            DataFormatter df = new DataFormatter();
            Log.d(TAG,"read file...");
            FileInputStream excelFile = new FileInputStream(new File(filePath));
            Workbook workbook = new XSSFWorkbook(excelFile);
            Sheet sheet = workbook.getSheetAt(0);
            Instant end2 = Instant.now();
            Log.d(TAG,"reading time= "+humanReadableFormat(Duration.between(start2, end2)));
            Log.d(TAG,"start parsing...");
            for (Row currentRow : sheet) {
                int j = 0;
                i++;
                if(i < 3) continue;
                //if(i > 4) break;
                List<String> data = new ArrayList<>();
                for (Cell currentCell : currentRow) {
                    data.add(df.formatCellValue(currentCell));
                    //System.out.print(df.formatCellValue(currentCell)+"   ");
                    j++;
                }
                //System.out.print(data.size()+"\n");
                if(data.size() < 16) continue;

                try{
                    result.add(new CSVTagData(data.get(0), data.get(1), Long.parseLong(data.get(2)), Integer.valueOf(data.get(3)),
                            Long.valueOf(data.get(4)), data.get(5),
                            data.get(6), data.get(7),
                            Integer.valueOf(data.get(8)), data.get(9),
                            data.get(10), data.get(11),
                            Integer.valueOf(data.get(12))
                    ));
                }catch (NumberFormatException e){
                    Log.e(TAG,"error while parsing data", e);
                }
            }
            //System.out.println(result);
        } catch (IOException e) {
            Log.e(TAG, "error while parsing xlsx file", e);
        }

        Instant end = Instant.now();
        Log.d(TAG,"parsing time= "+humanReadableFormat(Duration.between(start, end)));
        Log.d(TAG, String.format("found %d valid input.",result.size()));

        return result;
    }
}