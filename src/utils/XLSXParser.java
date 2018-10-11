package utils;

import database.DataBase;
import entities.ExcelDataRow;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import trikita.log.Log;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static utils.Utils.findAllFilesWithExt;
import static utils.Utils.humanReadableFormat;

/**
 * Created by Axel on 26/09/2018.
 * spreadsheet parsing class
 */
public class XLSXParser {

    private static String TAG = XLSXParser.class.getName();

    private static double ENTRY_COUNT = 0.0;

    public static void init(String dirPath, String dbName){
        Log.d(TAG,"init...");
        List<String> files = findAllFilesWithExt(dirPath);
        List<String> logs = new ArrayList<>();

        if(files.size() > 0){
            DataBase dataBase = new DataBase(dbName);
            Log.d(TAG,"new db created dbName="+dbName);
            for (String path: files) {
                Instant startCurrFileProcessing = Instant.now();
                double currFileEntry = 0;
                List<ExcelDataRow> entries = parse(path);
                List<List<List<ExcelDataRow>>> groups = groupEntries(entries);
                double currFileTotalEntries = entries.size();

                Instant startEntryProcessing = Instant.now(); Instant endEntryProcessing;

                for(List<List<ExcelDataRow>> day : groups){
                    dataBase.init(day.get(0).get(0));//create new collection for the day
                    double cptEntries = 0;
                    ENTRY_COUNT = currFileTotalEntries; //change this value to choose how many entries per days should be kept
                    for (List<ExcelDataRow> animal : day){
                        dataBase.addAnimal(animal.get(0));
                        for (ExcelDataRow entryRow : animal){
                            dataBase.addEntry(entryRow);
                            cptEntries++;
                            currFileEntry++;
                            endEntryProcessing = Instant.now();
                            System.out.println(String.format("progress: %.0f/%.0f  %d%%  %s", currFileEntry, currFileTotalEntries,
                                    (int)((currFileEntry/currFileTotalEntries)*100.0), humanReadableFormat(Duration.between(startEntryProcessing, endEntryProcessing))));
                            if(cptEntries > ENTRY_COUNT - 1){
                                cptEntries = 0;
                                break;
                            }
                        }
                    }
                }
                Instant endCurrFileProcessing = Instant.now();
                logs.add(String.format("%s to process file %s",
                        humanReadableFormat(Duration.between(startCurrFileProcessing, endCurrFileProcessing)), path));
            }
            System.out.println("**********Transfer to db finished**********");
            for (String log: logs) {System.out.println(log);}
            System.out.println("*******************************************");
        }else {
            Log.i(TAG,String.format("no files found in directory:\n%s",dirPath));
        }
    }

    private static List<List<List<ExcelDataRow>>> groupEntries(List<ExcelDataRow> entries){
        List<List<List<ExcelDataRow>>> sortedFinal = new ArrayList<>();
        Instant start = Instant.now();
        entries.sort(Comparator.comparing(ExcelDataRow::getTagSerialNumber));
        List<List<ExcelDataRow>> groupedByDate = entries.stream()
                .collect(Collectors.groupingBy(ExcelDataRow::getDate))
                .entrySet().stream()
                .map(e -> { List<ExcelDataRow> c = new ArrayList<>(); c.addAll(e.getValue()); return c; })
                .collect(Collectors.toList());

        for (List<ExcelDataRow> row: groupedByDate) {
            List<List<ExcelDataRow>> groupedBySerialNumber = row.stream()
                    .collect(Collectors.groupingBy(ExcelDataRow::getTagSerialNumber))
                    .entrySet().stream()
                    .map(e -> { List<ExcelDataRow> c = new ArrayList<>(); c.addAll(e.getValue()); return c; })
                    .collect(Collectors.toList());
            groupedBySerialNumber.sort(Comparator.comparing(e -> e.get(0).getDate()));
            sortedFinal.add(groupedBySerialNumber);
        }

        Instant end = Instant.now();
        Log.d(TAG,"sorting time "+humanReadableFormat(Duration.between(start, end)));
        return sortedFinal;
    }

    private static List<ExcelDataRow> parse(String filePath){
        Log.d(TAG, String.format("start parsing %s ...", filePath));
        List<ExcelDataRow> result = new ArrayList<>();
        Instant start = Instant.now();
        Instant start2 = Instant.now();
        try {
            DataFormatter df = new DataFormatter();
            Log.d(TAG,"reading file...");
            FileInputStream excelFile = new FileInputStream(new File(filePath));
            Workbook workbook = new XSSFWorkbook(excelFile);
            Sheet sheet = workbook.getSheetAt(0);
            Instant end2 = Instant.now();
            Log.d(TAG,"reading time "+humanReadableFormat(Duration.between(start2, end2)));
            Log.d(TAG,"start parsing...");
            int i = 0;
            for (Row currentRow : sheet) {
                //Log.d(TAG,"reading...");
                i++;
                if(i < 3) continue;//skip header
                List<String> data = new ArrayList<>();
                for (Cell currentCell : currentRow) {
                    data.add(df.formatCellValue(currentCell));
                    //System.out.print(df.formatCellValue(currentCell)+"   ");
                }
                //System.out.print(data.size()+"\n");
                if(data.get(4).length() != 11) continue;

                try{
                    result.add(new ExcelDataRow(data.get(0), data.get(1), Long.parseLong(data.get(2)), Integer.valueOf(data.get(3)),
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
        } catch (IOException e) {Log.e(TAG, "error while parsing xlsx file", e);}

        Instant end = Instant.now();
        Log.d(TAG,"parsing time "+humanReadableFormat(Duration.between(start, end)));
        Log.d(TAG, String.format("found %d valid input.",result.size()));

        return result;
    }
}