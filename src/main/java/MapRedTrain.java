import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by omart on 2/8/2017.
 */
public class MapRedTrain {

    private final String delimiters = "[ \t,;\\.\\?\\!-:@\\[\\](){}_\\*/]";
    private final List<String> stopWords = new ArrayList<>();

    private enum FilePath {
        DELIMITER_FILE("tmp/delimiter.txt"),
        STOP_WORD_FILE("tmp/stopwords.txt"),
        TITLE_FILE("tmp/title-a");

        private String path;

        FilePath(String path) {
            this.path = path;
        }

        String getPath() {
            return path;
        }
}

    public String loadFile(String path) {
        String line;
        StringBuffer everything = new StringBuffer();

        // Populate Stop words
        try {
            try(BufferedReader br = new BufferedReader(new FileReader(path))){
                while((line = br.readLine()) != null)  {
                    everything.append(line);
                    everything.append("\n");
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return everything.toString();
    }

    public static void main(String[] args) {

        MapRedTrain mrt = new MapRedTrain();

        List<String> stopWords = Arrays.asList(mrt.loadFile(FilePath.STOP_WORD_FILE.getPath()).split("\n"));
//        String delimiters = mrt.loadFile(FilePath.STOP_WORD_FILE.getPath());
//
        System.out.println(stopWords);
//        System.out.println(delimiters);


    }
}
