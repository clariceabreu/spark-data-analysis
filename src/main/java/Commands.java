import java.util.*;

public class Commands {
    private static final String PRINT_GREEN = "\033[92m";
    private static final String PRINT_YELLOW = "\033[1;93m";
    private static final String PRINT_RED = "\u001b[31m";
    private static final String PRINT_BLUE = "\u001b[34;1m";
    private static final String PRINT_COLOR_END = "\033[0m";

    private DataAnalysis dataAnalysis;
    private String method = "";
    private DatasetColumn column;
    private HashSet<String> filter = new HashSet<>();;


    public Commands(DataAnalysis dataAnalysis) {
        this.dataAnalysis = dataAnalysis;

        System.out.println("Write a command or type "+ PRINT_GREEN + "help" + PRINT_COLOR_END + " to show the available commands:");
        Scanner scanner = new Scanner(System.in);
        String command = scanner.nextLine();

        while (!command.equals("quit")) {
            executeCommand(command);
            System.out.println();
            System.out.println("Write a command or type help to show the available commands:");
            command = scanner.nextLine();
        }
    }

    private void executeCommand(String command) {
        if (command.equals("help")) {
            printHelp();
            return;
        }

        try {
            this.method = command.split(" ")[0];
            this.column = DatasetColumn.valueOf(command.split(" ")[1]);

            if (command.contains("--filter")) {
                String values = command.split("=")[1];
                values = values.replaceAll(" ", "");
                this.filter = new HashSet<>(Arrays.asList(values.split(",")));
            }
        } catch (IllegalArgumentException e) {
            System.out.println(PRINT_RED + "Error: invalid column");
            System.out.println("The column name should be exactly the same as the dataset" + PRINT_COLOR_END);
            System.out.println();
            return;
        } catch (Exception e) {
            System.out.println(PRINT_RED + "Error: invalid command" + PRINT_COLOR_END);
            System.out.println();
            printHelp();
            return;
        }

        switch (this.method) {
            case "mean":
                dataAnalysis.mean(this.column, this.filter);
                break;
            case "standard-deviation":
                dataAnalysis.standardDeviation(this.column, this.filter);
                break;
            case "linear-regression":
                dataAnalysis.regression(DatasetColumn.DEWP, DatasetColumn.TEMP, this.filter);
            default:
                printHelp();
        }
    }


    private void printHelp() {
        System.out.println("Available commands:");
        System.out.println("- " + PRINT_GREEN + "mean" + PRINT_YELLOW + " [column]" + PRINT_BLUE +  " [--filter stations]" + PRINT_COLOR_END);
        System.out.println("↳ Example: " + PRINT_GREEN + "mean TEMP --filter=1001499999,1001099999" + PRINT_COLOR_END);
        System.out.println();
        System.out.println("- " + PRINT_GREEN + "standard-deviation" + PRINT_YELLOW + " [column]" + PRINT_BLUE +  " [--filter stations]" + PRINT_COLOR_END);
        System.out.println("↳ Example: " + PRINT_GREEN + "standard-deviation DEWP --filter=1001499999" + PRINT_COLOR_END);
        System.out.println();
        System.out.println("- " + PRINT_GREEN + "help" + PRINT_COLOR_END + ": show available commands");
        System.out.println();
        System.out.println("- " + PRINT_GREEN + "quit" + PRINT_COLOR_END + ": ends program");
        System.out.println();
    }
}
