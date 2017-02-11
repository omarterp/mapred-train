import java.io.*;
import java.util.*;

/**
 * Created by omart on 2/8/2017.
 */
public class MapRedTrain {

    private String delimiters; //"[ \t,;\\.\\?\\!-:@\\[\\](){}_\\*/]";
    private List<String> stopWords = new ArrayList<>();
    private Map<String, List<Integer>> words = new HashMap<>();

    private enum FilePath {
        DELIMITER_FILE("tmp/delimiters.txt"),
        STOP_WORD_FILE("tmp/stopwords.txt"),
        TITLE_FILE("tmp/titles-a");

        private String path;

        FilePath(String path) {
            this.path = path;
        }

        String getPath() {
            return path;
        }
}

    public boolean setup() {
        stopWords = Arrays.asList(loadFile(FilePath.STOP_WORD_FILE.getPath()).split("\n"));
        delimiters = loadFile(FilePath.DELIMITER_FILE.getPath());

        return true;
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

    public Map<String, List<Integer>> map(String inputFile) {

        String line;

        try {
            try(BufferedReader br = new BufferedReader(new FileReader(inputFile))) {
                while((line = br.readLine()) != null) {
                    // TODO Mapper Code Begins here
                    // Split the line based on delimiters

                    StringTokenizer tokens = new StringTokenizer(line, delimiters);
                    while(tokens.hasMoreTokens()) {
                        String token = tokens.nextToken().trim().toLowerCase();
                        if(!stopWords.contains(token.trim().toLowerCase())) {
                            if(!words.containsKey(token)) {
                                List<Integer> values = new ArrayList<>();
                                values.add(1);
                                words.put(token, values);
                            }
                            else {
                                words.get(token).add(1);
                            }
                            //System.out.println(token);
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        for(Map.Entry<String, List<Integer>> entry : words.entrySet()) {
            System.out.println(entry.getKey() + ": " + entry.getValue());
        }

        return words;
    }

    /**
     * Reduces the mapped words and frequency
     *
     * @return KV Pair of word and frequency
     */
    public Map<String, Integer> reduce() {
        Map<String, Integer> wordCounts = new HashMap<>();

        // Iterate over the words object and sum word occurrences
        for(Map.Entry<String, List<Integer>> word : words.entrySet()) {
            int freq = 0;

            for(int hit : word.getValue()) {
                freq += 1;
            }
            wordCounts.put(word.getKey(), freq);
        }

        return wordCounts;
    }

    public static void main(String[] args) {

        MapRedTrain mrt = new MapRedTrain();
        mrt.setup();

        mrt.map(FilePath.TITLE_FILE.getPath());
        System.out.println(mrt.reduce());

//        List<String> stopWords = Arrays.asList(mrt.loadFile(FilePath.STOP_WORD_FILE.getPath()).split("\n"));
//        String delimiters = mrt.loadFile(FilePath.DELIMITER_FILE.getPath());

        System.out.println(mrt.stopWords);
        System.out.println(mrt.delimiters);


    }
}
