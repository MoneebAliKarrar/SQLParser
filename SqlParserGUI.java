import javafx.application.Application;
import javafx.geometry.Insets;
import javafx.scene.Scene;
import javafx.scene.control.*;
import javafx.scene.layout.BorderPane;
import javafx.stage.Stage;


public class SqlParserGUI extends Application {


    private TextArea inputTextArea;
    private TextArea outputTextArea;

    public static void main(String[] args) {
        launch(args);
    }

    @Override
    public void start(Stage primaryStage) {

        primaryStage.setTitle("SQL Parser");

        // Input TextArea
        inputTextArea = new TextArea();
        inputTextArea.setPromptText("Enter SQL command here...");

        // Output TextArea
        outputTextArea = new TextArea();
        outputTextArea.setEditable(false);
        outputTextArea.setWrapText(true);

        // Button to Parse SQL
        Button parseButton = new Button("Parse SQL");
        parseButton.setOnAction(e -> parseSQLCommand());

        // Layout
        BorderPane layout = new BorderPane();
        layout.setPadding(new Insets(10));
        layout.setTop(inputTextArea);
        layout.setCenter(parseButton);
        layout.setBottom(outputTextArea);

        Scene scene = new Scene(layout, 600, 400);
        primaryStage.setScene(scene);
        primaryStage.show();
    }

    private void parseSQLCommand() {
        String userInput = inputTextArea.getText();
        String result = SqlParser.parseAndExecute(userInput);
        outputTextArea.setText("Parsed SQL:\n" + result);
    }
}
    

