package utils;

public class Args {
    public String inputDirPath;
    public String dbName;
    public boolean debug;
    public Args(String inputDirPath, String dbName, boolean debug) {
        this.inputDirPath = inputDirPath;
        this.dbName = dbName;
        this.debug = debug;
    }
}
