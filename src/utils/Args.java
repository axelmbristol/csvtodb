package utils;

public class Args {
    public String inputDirPath;
    public int type;
    public boolean debug;
    public Args(String inputDirPath, int type, boolean debug) {
        this.inputDirPath = inputDirPath;
        this.type = type;
        this.debug = debug;
    }
}
