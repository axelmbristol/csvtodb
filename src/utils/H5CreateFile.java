package utils;

import entities.ExcelDataRow;
import trikita.log.Log;
import as.hdfql.*;
import java.util.List;

public class H5CreateFile {

    private static String TAG = H5CreateFile.class.getName();


    public H5CreateFile(String fileName) {
        // load HDFql shared library (make sure it can be found by the JVM)
        System.loadLibrary("HDFql");

        // create an HDF5 file named "test.h5"
        HDFql.execute("CREATE FILE data.h5");
    }

    public void addEntry(List<ExcelDataRow> excelDataRows){
        HDFql.execute("USE FILE data.h5");
        HDFql.execute("CREATE GROUP farm1");
        HDFql.execute("CREATE DATASET farm1/animal1 AS INT(3, 2)");
        HDFql.execute("INSERT INTO farm1/animal1 VALUES((7, 8), (3, 5), (4,1))");
        System.exit(0);
    }
}
