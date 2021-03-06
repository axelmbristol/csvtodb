package utils;

import database.CassandraDataBase;
import database.HDF5;
import database.MongoDataBase;
import entities.Day;
import entities.ExcelDataRow;
import org.apache.poi.openxml4j.exceptions.NotOfficeXmlFileException;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import trikita.log.Log;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

import static utils.Utils.*;

/**
 * Created by Axel on 26/09/2018.
 * spreadsheet parsing class
 */
public class XLSXParser {

    private static String TAG = XLSXParser.class.getName();

    private static double ENTRY_COUNT = 0.0;
    private static int spreadSheetType = 0;
    private static double transferedRowCount = 0.0;

    public static void init(String dirPath, int type){
        Log.d(TAG,"init...");
        writeToLogFile("init");
        List<String> files = findAllFilesWithExt(dirPath);
        List<String> logs = new ArrayList<>();

        if(files.size() > 0){

            try {

                double currFileEntry = 0;
                Instant startEntryProcessing = Instant.now(); Instant endEntryProcessing;
                switch (type){
                    case 0:
                        Log.d(TAG,"database type 0 -> MongoDB.");
                        MongoDataBase dataBase = new MongoDataBase();
                        //Log.d(TAG,"new db created dbName="+dbName);
                        for (String path: files) {
                            Instant startCurrFileProcessing = Instant.now();
                            List<ExcelDataRow> entries = parse(path);
                            List<Day> groups = groupEntries(entries);
                            for (Day day : groups) {
                                dataBase.addEntry(day, day.data.get(0).get(0).getControlStation().toString());
                            }
                            currFileEntry++;
                            endEntryProcessing = Instant.now();
                            System.out.println(String.format("progress: %.0f/%.0f  %d%%  %s %d", currFileEntry, (float) files.size(),
                                    (int) ((currFileEntry / files.size()) * 100.0),
                                    humanReadableFormat(Duration.between(startEntryProcessing, endEntryProcessing)), (int)transferedRowCount));

                            Instant endCurrFileProcessing = Instant.now();
                            String log = String.format("%s to process file %s",
                                    humanReadableFormat(Duration.between(startCurrFileProcessing, endCurrFileProcessing)), path);
                            logs.add(log);
                            writeToLogFile(log);
                            transferedRowCount += entries.size();
                            entries.clear();
                            groups.clear();
                        }
                    case 1:
                        Log.d(TAG,"database type 1 -> Cassandra.");
                        //MongoDataBase dataBase = new MongoDataBase();
                        CassandraDataBase cassandraDataBase = new CassandraDataBase();
                        //Log.d(TAG,"new db created dbName="+dbName);
                        for (String path: files) {
                            Instant startCurrFileProcessing = Instant.now();
                            List<ExcelDataRow> entries = parse(path);
                            cassandraDataBase.addEntry(entries);
                            transferedRowCount += entries.size();
                            currFileEntry++;
                            endEntryProcessing = Instant.now();
                            System.out.println(String.format("progress: %.0f/%.0f  %d%%  %s  %d", currFileEntry, (float)files.size(),
                                    (int)((currFileEntry/files.size())*100.0),
                                    humanReadableFormat(Duration.between(startEntryProcessing, endEntryProcessing)), (int)transferedRowCount));

                            Instant endCurrFileProcessing = Instant.now();
                            String log = String.format("%s to process file %s",
                                    humanReadableFormat(Duration.between(startCurrFileProcessing, endCurrFileProcessing)), path);
                            logs.add(log);
                            writeToLogFile(log);
                            entries.clear();
                        }

                    case 2:
                        Log.d(TAG,"database type 2 -> hdf5.");
                        HDF5 h5 = new HDF5("data.h5");
                        for (String path: files) {
                            Instant startCurrFileProcessing = Instant.now();
                            List<ExcelDataRow> entries = parse(path);
                            h5.addEntry(entries);
                            transferedRowCount += entries.size();
                            currFileEntry++;
                            endEntryProcessing = Instant.now();
                            System.out.println(String.format("progress: %.0f/%.0f  %d%%  %s  %d", currFileEntry, (float)files.size(),
                                    (int)((currFileEntry/files.size())*100.0),
                                    humanReadableFormat(Duration.between(startEntryProcessing, endEntryProcessing)), (int)transferedRowCount));

                            Instant endCurrFileProcessing = Instant.now();
                            String log = String.format("%s to process file %s",
                                    humanReadableFormat(Duration.between(startCurrFileProcessing, endCurrFileProcessing)), path);
                            logs.add(log);
                            writeToLogFile(log);
                            entries.clear();
                            break;
                        }

                    default:
                        Log.d(TAG,"database type deafault.");
                        break;

                }
            }catch (OutOfMemoryError e){
                Log.e(TAG,"error while processing file", e);
            }


            System.out.println("**********Transfer to db finished**********");
            for (String log: logs) {
                System.out.println(log);
            }
            System.out.println("total treated row count ="+ (int)transferedRowCount);
            System.out.println("*******************************************");
        }else {
            Log.i(TAG,String.format("no files found in directory:\n%s",dirPath));
        }
    }

    private static List<Day> groupEntries(List<ExcelDataRow> entries){
        List<Day> sortedFinal = new ArrayList<>();
        Instant start = Instant.now();
        entries.sort(Comparator.comparing(ExcelDataRow::getDate));

        entries.sort(Comparator.comparing(ExcelDataRow::getTagSerialNumber));
        List<List<ExcelDataRow>> groupedByDate = entries.stream()
                .collect(Collectors.groupingBy(ExcelDataRow::getDate))
                .entrySet().stream()
                .map(e -> {
                    List<ExcelDataRow> c = new ArrayList<>(e.getValue());return c; })
                .collect(Collectors.toList());
        //groupedByDate.sort(Comparator.comparing(e -> e.get(0).getDate()));

        //Collections.sort(groupedByDate, Comparator.comparing(a -> a.get(0).getDate()));

        for (List<ExcelDataRow> row: groupedByDate) {
            List<List<ExcelDataRow>> groupedBySerialNumber = row.stream()
                    .collect(Collectors.groupingBy(ExcelDataRow::getTagSerialNumber))
                    .entrySet().stream()
                    .map(e -> {
                        List<ExcelDataRow> c = new ArrayList<>(e.getValue());
                        c.sort(Comparator.comparing(ExcelDataRow::getEpoch));
                        return c; })
                    .collect(
                            Collectors.toList()
                    );

            sortedFinal.add(new Day(dateFromString(row.get(0).getDate()), row.get(0).getControlStation(), groupedBySerialNumber));
        }

        Collections.sort(sortedFinal, Comparator.comparing(s -> s.epoch));

        //Collections.sort(groupedByDate, Comparator.comparing(a -> a.get(0).getDate()));

        Instant end = Instant.now();
        Log.d(TAG,"sorting time "+humanReadableFormat(Duration.between(start, end)));
        return sortedFinal;
    }

    private static int getSpreadSheetDataType(String inputDate){
        try {
            String year = inputDate.split(" ")[0].split("/")[2];
            return ((Integer.valueOf(year) >= 13)? 2:1);
        }catch (ArrayIndexOutOfBoundsException e){
            return 1;
        }
    }

    private static List<ExcelDataRow> parse(String filePath){
        Log.d(TAG, String.format("start parsing %s ...", filePath));
        List<ExcelDataRow> result = new ArrayList<>();
        Instant start = Instant.now();
        Instant start2 = Instant.now();
        try {
            DataFormatter df = new DataFormatter();
            Log.d(TAG, "reading file...");
            FileInputStream excelFile = new FileInputStream(new File(filePath));
            Log.d(TAG, "reading file...");
            Workbook workbook = new XSSFWorkbook(excelFile);
            Log.d(TAG, "reading file...");


            for (int n = 0; n < workbook.getNumberOfSheets(); n++) {

                Sheet sheet = workbook.getSheetAt(n);

                Instant end2 = Instant.now();
                Log.d(TAG, "reading time " + humanReadableFormat(Duration.between(start2, end2)));
                Log.d(TAG, "start parsing "+filePath);
                int i = 0;

                spreadSheetType = getSpreadSheetDataType(df.formatCellValue(sheet.getRow(4).getCell(0)));

                Log.d(TAG, "start parsing...");
                for (Row currentRow : sheet) {
                    //Log.d(TAG,"reading...");
                    i++;
                    //if(i < 3) continue;//skip header
                    List<String> data = new ArrayList<>();
                    for (Cell currentCell : currentRow) {
                        data.add(df.formatCellValue(currentCell));
                        //  System.out.print(df.formatCellValue(currentCell)+"   ");
                    }
                    //System.out.print(data.size()+"\n");

                    if (data.size() < 4) continue;

                    String date = data.get(0);
                    DateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
                    Date startDate;
                    try {
                        startDate = dateFormat.parse(date);
                    } catch (ParseException e) {
                        Log.e(TAG, "invalid date", e);
                        continue;
                    }

                    if (spreadSheetType == 1) {

                        if (data.size() == 5) {
                            //Log.d(TAG,"add data to result");
                            try {
                                result.add(new ExcelDataRow(prettyDate(startDate), data.get(1), Long.parseLong(data.get(2)), Long.valueOf(data.get(3)),
                                        null, null, Integer.valueOf(data.get(4)), filePath));
                            } catch (NumberFormatException | IndexOutOfBoundsException e) {
                                Log.e(TAG, "error while parsing data type=" + spreadSheetType + " data=" + data, e);
                                //writeToLogFile("error while parsing data type="+spreadSheetType+" data="+data+" e="+e.getMessage()+" filepath="+filePath);
                                continue;
                            }
                        } else {
                            if (data.get(3).length() != 11 | !isNumeric(data.get(6)) | !isTimeValid(data.get(1))){
                                Log.d(TAG,"invalid data "+data);
                                continue;
                            }
                            try {
                                result.add(new ExcelDataRow(prettyDate(startDate), data.get(1), Long.parseLong(data.get(2)), Long.valueOf(data.get(3)),
                                        data.get(4), data.get(5), Integer.valueOf(data.get(6)), filePath));
                            } catch (NumberFormatException | IndexOutOfBoundsException e) {
                                Log.e(TAG, "error while parsing data type=" + spreadSheetType + " data=" + data, e);
                                writeToLogFile("error while parsing data type=" + spreadSheetType + " data=" + data + " e=" + e.getMessage() + " filepath=" + filePath);
                                continue;
                            }
                        }
                    }

                    if (spreadSheetType == 2) {
                        if (data.get(4).length() != 11 | !isNumeric(data.get(8)) | !isTimeValid(data.get(1))) {
                            Log.d(TAG,"invalid data "+data);
                            continue;
                        }
                        if (data.size() == 11) {
                            //Log.d(TAG,"add data to result");
                            try {
                                result.add(new ExcelDataRow(prettyDate(startDate), data.get(1), Long.parseLong(data.get(2)), Integer.valueOf(data.get(3)),
                                        Long.valueOf(data.get(4)), data.get(5),
                                        data.get(6),
                                        data.get(8),
                                        Integer.valueOf(data.get(8)),
                                        data.get(10),
                                        data.get(10),
                                        "",
                                        null,
                                        filePath
                                ));
                            } catch (NumberFormatException | IndexOutOfBoundsException e) {
                                Log.e(TAG, "error while parsing data type=" + spreadSheetType + " data=" + data + " file=" + filePath, e);
                                //writeToLogFile("error while parsing data type="+spreadSheetType+" data="+data+" e="+e.getMessage()+" filepath="+filePath);
                                continue;
                            }
                        } else {
                            try {
                                result.add(new ExcelDataRow(prettyDate(startDate), data.get(1), Long.parseLong(data.get(2)), Integer.valueOf(data.get(3)),
                                        Long.valueOf(data.get(4)), data.get(5),
                                        data.get(6), data.get(7),
                                        Integer.valueOf(data.get(8)), data.get(9),
                                        data.get(10), data.get(11),
                                        Integer.valueOf(data.get(12)),
                                        filePath
                                ));
                            } catch (NumberFormatException | IndexOutOfBoundsException e) {
                                Log.e(TAG, "error while parsing data type=" + spreadSheetType + " data=" + data + " file=" + filePath, e);
                                writeToLogFile("error while parsing data type=" + spreadSheetType + " data=" + data + " e=" + e.getMessage() + " filepath=" + filePath);
                                continue;
                            }
                        }

                    }

                }

            }
            workbook.close();
        } catch (NullPointerException  | IndexOutOfBoundsException | NotOfficeXmlFileException | IOException e) {
            Log.e(TAG, "error while parsing xlsx file", e);
            writeToLogFile("error while parsing data type="+spreadSheetType+" e="+e.getMessage()+" filepath="+filePath);
        }

        Instant end = Instant.now();
        Log.d(TAG,"parsing time "+humanReadableFormat(Duration.between(start, end)));
        Log.d(TAG, String.format("found %d valid input.",result.size()));

        return result;
    }
}