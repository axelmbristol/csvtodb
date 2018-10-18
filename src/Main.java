import org.apache.commons.cli.*;
import utils.Args;
import utils.XLSXParser;


public class Main {

    public static void main(String[] args) {
        System.out.println(
                " _____ _____ _____ _____ _____ ____  _____ \n" +
                "|     |   __|  |  |_   _|     |    \\| __  |\n" +
                "|   --|__   |  |  | | | |  |  |  |  | __ -|\n" +
                "|_____|_____|\\___/  |_| |_____|____/|_____|\n" +
                "                                           ");
        System.out.println("Accitrack spreadsheet parser.\n" +
                "This program transfers the data stored in excel format to a MongoDB database.");
        System.out.println("Axel Montout @ University of bristol. Copyright 2018.\n");
        Args arguments = initCommandParser(args);
        XLSXParser.init(arguments.inputDirPath,arguments.dbName);
    }

    private static Args initCommandParser(String[] args){
        Options options = new Options();
        Option input = new Option("dir", "directory", true, "input directory path.");
        input.setRequired(true);
        options.addOption(input);
        Option dbName = new Option("n", "name", true, "name of mongoDB database.");
        dbName.setRequired(true);
        options.addOption(dbName);
        Option output = new Option("d", "debug", false, "enables log output.");
        output.setRequired(false);
        options.addOption(output);
        CommandLineParser parser = new BasicParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            formatter.printHelp("csvtodb", options);
            System.exit(1);
        }
        return new Args(cmd.getOptionValue("directory") , cmd.getOptionValue("name"),
                (cmd.hasOption('d')| cmd.hasOption("debug")));
    }
}
