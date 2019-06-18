import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;

public class MolocoQ2 {
    // The hash map dictionary which stores the mapping between product_id and its corresponding quantity sold
    private static Map<String, Integer> productCountMap = new HashMap<>();

    // The hash map dictionary which stores the mapping between product_id and its corresponding set of user_id who
    // purchased it
    private static Map<String, Set<String>> productUserMap = new HashMap<>();

    /**
     * Used to get ranking based on the unique number of users who purchased each product
     *
     * @return the list of products with maximum number of unique users who purchased it
     */
    private static List<String> getProductBasedOnNumberOfPurchasers() {
        List<String> result = new ArrayList<>();
        int max = Integer.MIN_VALUE;
        for (Set<String> users : productUserMap.values()) {
            if (users.size() > max) {
                max = users.size();
            }
        }
        for (String pid : productUserMap.keySet()) {
            if (productUserMap.get(pid).size() == max) {
                result.add(pid);
            }
        }
        return result;
    }

    /**
     * Used to get ranking based on the total quantity of each product sold
     *
     * @return the list of products with the most quantity sold
     */
    private static List<String> getProductBasedOnQuantity() {
        List<String> result = new ArrayList<>();
        int max = Collections.max(productCountMap.values());
        for (String pid : productCountMap.keySet()) {
            if (productCountMap.get(pid) == max) {
                result.add(pid);
            }
        }
        return result;
    }

    /**
     * Read the source CSV file and process it
     *
     * @param path is the absolute path of the CSV file which has original data
     * @return the list of string where each string represents a row of the CSV file data. Each string is the JSON
     * object message
     */
    private static List<String> readFileFromSource(String path) {
        List<String> records = new ArrayList<>();
        try (Scanner scanner = new Scanner(new File(path))) {
            while (scanner.hasNextLine()) {
                records.add(getRecordFromLine(scanner.nextLine()));
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        return records;
    }

    /**
     * Manipulate and transform the raw data row into a cleaner format
     *
     * @param line is the raw String of data
     * @return processed String of JSON object by delimiting by comma
     */
    private static String getRecordFromLine(String line) {
        StringBuilder values = new StringBuilder();
        try (Scanner rowScanner = new Scanner(line)) {
            rowScanner.useDelimiter(",");
            while (rowScanner.hasNext()) {
                values.append(rowScanner.next().replace("\"", ""));
                if (rowScanner.hasNext()) {
                    values.append(",");
                }
            }
        }
        return values.toString();
    }

    /**
     * Set the hash maps for the product_id-quantity and product_id-number of users from the processed data of CSV
     *
     * @param records list of string where each string represents a row of the CSV file data. Each string is the JSON
     *                object message
     */
    private static void setMappings(List<String> records) {
        for (String obj : records) {
            // remove extraneous symbols and brackets
            obj = obj.replace("{", "").replace("}", "");
            String[] parts = obj.split(",");
            String UID = parts[0].split(": ")[1];
            String PID = parts[1].split(": ")[1];
            String quantity = parts[2].split(": ")[1];

            // make entry for the product_id and its quantity. If the product_id already exists in map, update its
            // quantity
            productCountMap.put(PID, productCountMap.getOrDefault(PID, 0) + Integer.parseInt(quantity));

            // make entry for the product_id along with the users who purchased it. If the product_id already exists,
            // update the count of the users who purchased it
            if (productUserMap.containsKey(PID)) {
                Set<String> userSet = productUserMap.get(PID);
                userSet.add(UID);
                productUserMap.put(PID, userSet);
            } else {
                Set<String> userSet = new HashSet<>();
                userSet.add(UID);
                productUserMap.put(PID, userSet);
            }
        }
    }

    /*
    Given this as input (assume that it is a text file stored in your local machine), write a program that reads the file,
    and computes the most popular products based on two ranking methods.

    (1) Based on the unique number of users who purchased each product, and
    (2) Based on the total quantity of each product sold.
     */
    public static void main(String[] args) {
        File file = new File("src/data2.csv");
        String csvFile = file.getAbsolutePath();
        List<String> records = readFileFromSource(csvFile);
        setMappings(records);
        System.out.println(getProductBasedOnNumberOfPurchasers());
        System.out.println(getProductBasedOnQuantity());
    }
}
