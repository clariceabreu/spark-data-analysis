import java.util.*;

public class Commands {
    private static final String PRINT_GREEN = "\033[92m";
    private static final String PRINT_YELLOW = "\033[1;93m";
    private static final String PRINT_RED = "\u001b[31m";
    private static final String PRINT_BLUE = "\u001b[34;1m";
    private static final String PRINT_CYAN = "\u001b[36m";
    private static final String PRINT_COLOR_END = "\033[0m";

    private DataAnalysis dataAnalysis;
    private String method = "";
    private DatasetColumn column;
    private HashSet<String> filter = new HashSet<>();


    public Commands(DataAnalysis dataAnalysis) {
        this.dataAnalysis = dataAnalysis;

        System.out.println("Write a command or type "+ PRINT_GREEN + "help" + PRINT_COLOR_END + " to show the available commands:" + PRINT_CYAN);
        Scanner scanner = new Scanner(System.in);
        System.out.print("> ");
        System.out.flush();
        String command = scanner.nextLine();
        System.out.println(PRINT_COLOR_END);

        while (!command.equals("quit")) {
            executeCommand(command);
            System.out.println("Write a command or type help to show the available commands:" + PRINT_CYAN);
            System.out.print("> ");
            System.out.flush();
            command = scanner.nextLine();
            System.out.println(PRINT_COLOR_END);
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
            filter = new HashSet<>();

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
                DatasetColumn columnToBasePrediction = DatasetColumn.valueOf(command.split(" ")[2]);
                Double observedValue = Double.parseDouble(command.split(" ")[3]);
                dataAnalysis.regression(columnToBasePrediction, column, observedValue, this.filter);
                break;
            default:
                printHelp();
        }
    }

    private void printHelp() {
        System.out.println("Available commands:");
        System.out.println("- " + PRINT_GREEN + "mean" + PRINT_YELLOW + " [column]" + PRINT_BLUE +  " [--filter stations]" + PRINT_COLOR_END);
        System.out.println("↳ Example: " + PRINT_CYAN + "mean TEMP --filter=1001499999,1001099999" + PRINT_COLOR_END);
        System.out.println();
        System.out.println("- " + PRINT_GREEN + "standard-deviation" + PRINT_YELLOW + " [column]" + PRINT_BLUE +  " [--filter stations]" + PRINT_COLOR_END);
        System.out.println("↳ Example: " + PRINT_CYAN + "standard-deviation DEWP --filter=1001499999" + PRINT_COLOR_END);
        System.out.println();
        System.out.println("- " + PRINT_GREEN + "linear-regression" + PRINT_YELLOW + " [column_to_predict] [column_to_base_prediction] [observed_value]" + PRINT_BLUE +  " [--filter stations]" + PRINT_COLOR_END);
        System.out.println("↳ Example: " + PRINT_CYAN + "linear-regression TEMP DEWP 49.5 --filter=03005099999" + PRINT_COLOR_END);
        System.out.println();
        System.out.println("- " + PRINT_GREEN + "help" + PRINT_COLOR_END + ": show available commands");
        System.out.println();
        System.out.println("- " + PRINT_GREEN + "quit" + PRINT_COLOR_END + ": ends program");
        System.out.println();
    }
}
