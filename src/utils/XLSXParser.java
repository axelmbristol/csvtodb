package utils;

import entities.CSVTagData;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import trikita.log.Log;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static utils.Utils.findAllFilesWithExt;

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
        try {
            FileInputStream excelFile = new FileInputStream(new File(filePath));
            Workbook workbook = new XSSFWorkbook(excelFile);
            Sheet datatypeSheet = workbook.getSheetAt(0);
            for (Row currentRow : datatypeSheet) {
                for (Cell currentCell : currentRow) {

                    Log.d(TAG,"currentCell="+currentCell.getStringCellValue());
                    //getCellTypeEnum shown as deprecated for version 3.15
                    //getCellTypeEnum ill be renamed to getCellType starting from version 4.0
                       /* if (currentCell.getCellTypeEnum() == CellType.STRING) {
                            System.out.print(currentCell.getStringCellValue() + "--");
                        } else if (currentCell.getCellTypeEnum() == CellType.NUMERIC) {
                            System.out.print(currentCell.getNumericCellValue() + "--");
                        }
                        */
                }
            }
        } catch (IOException e) {
            Log.e(TAG, "error while parsing xlsx file", e);
        }

        return Collections.emptyList();
    }
}
