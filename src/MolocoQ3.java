import javafx.util.Pair;

import java.io.File;
import java.io.FileNotFoundException;
import java.sql.*;
import java.util.*;

import static java.util.stream.Collectors.toMap;

public class MolocoQ3 {
    // JDBC driver name and database URL
    static final String DB_URL = "jdbc:mysql://localhost/";
    static final String configs = "?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC";

    //  Database credentials
    static final String USER = "root";
    static final String PASS = "root@123";

    /**
     * Instantiates connection with the MySQL database on localhost:3306
     *
     * @return Connection object which is not null upon successful connection
     */
    private static Connection connectToDB() {
        Connection conn = null;
        Statement stmt;
        try {
            //STEP 1: Open a connection
            System.out.println("Connecting to database...");
            conn = DriverManager.getConnection(DB_URL + configs, USER, PASS);

            //STEP 2: Execute a query
            System.out.println("Creating database if it does not exist...");
            stmt = conn.createStatement();

            String createDatabaseQuery = "CREATE DATABASE IF NOT EXISTS sample";
            stmt.executeUpdate(createDatabaseQuery);
            System.out.println("Created Database \"sample\" successfully");
            conn = DriverManager.getConnection(DB_URL + "sample" + configs, USER, PASS);
        } catch (Exception se) {
            // Handle errors for JDBC
            se.printStackTrace();
        }
        return conn;
    }

    /**
     * Read the source CSV file and process it
     *
     * @param path is the absolute path of the CSV file which has original data
     * @return a list of string where each string represents a row of the CSV file data
     */
    private static List<String> readFileFromSource(String path) {
        List<String> parsedData = new ArrayList<>();
        try (Scanner scanner = new Scanner(new File(path))) {
            while (scanner.hasNextLine()) {
                parsedData.add(getRecordFromLine(scanner.nextLine()));
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        parsedData.remove(0); // remove header
        return parsedData;
    }

    /**
     * Manipulate and transform the raw data row into a cleaner format
     *
     * @param line is the raw String of data
     * @return processed String of data by delimiting by space
     */
    private static String getRecordFromLine(String line) {
        StringBuilder values = new StringBuilder();
        try (Scanner rowScanner = new Scanner(line)) {
            rowScanner.useDelimiter(",");
            while (rowScanner.hasNext()) {
                values.append(rowScanner.next());
                if (rowScanner.hasNext()) {
                    values.append(" ");
                }
            }
        }
        return values.toString().trim();
    }

    /**
     * Creates a table with given table name in the database to which the application is connected to
     *
     * @param connection is the connection object with live connection to MySQL database
     * @param tableName  is name of the table
     */
    private static void createTable(Connection connection, String tableName) {
        String createDataTable = "CREATE TABLE IF NOT EXISTS " + tableName + " (ts DATETIME NOT NULL , user_id VARCHAR(255) NOT NULL, country_id VARCHAR(255) NOT NULL, site_id VARCHAR(255) NOT NULL)";
        try {
            Statement createStatement = connection.createStatement();
            createStatement.executeUpdate(createDataTable);
            System.out.println("Created Table \"" + tableName + "\" successfully");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * Populates the created sample table with the processed CSV data by transforming the row data into MySQL
     * compatible format
     *
     * @param connection is the connection object with live connection to MySQL database
     * @param records    is the list of processed CSV data
     */
    private static void insertValuesIntoTable(Connection connection, List<String> records) {
        int count = 0;
        try (Statement statement = connection.createStatement()) {
            for (String record : records) {
                String[] parts = record.split(" ");
                String timeStamp = parts[0].replace("-", "/") + " " + parts[1];
                String userID = parts[2];
                String countryID = parts[3];
                String siteID = parts[4];
                String query = "INSERT INTO data_table VALUES (STR_TO_DATE(\'" + timeStamp + "\', '%Y/%m/%d %H:%i:%s'), \'" + userID + "\', \'" + countryID + "\', \'" + siteID + "\')";
                statement.executeUpdate(query);
                count++;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        System.out.println("Inserted" + count + " rows of CSV data successfully");
    }

    /**
     * Used to get the number of unique users for each site_id based on a give country_id
     *
     * @param connection is the connection object with live connection to MySQL database
     * @param tableName  is name of the table
     * @param country    is the country_id which we need to consider for querying the sub-data
     * @return the site_id with the maximum unique users from a given country
     */
    private static Map<String, Integer> getSiteWithLargestUsersInCountry(Connection connection, String tableName, String country) {
        Map<String, Integer> result = new HashMap<>();
        Map<String, Integer> siteUserCountMap = new HashMap<>();
        String maxSiteId = "";
        Set<String> numberOfUsers = new HashSet<>();
        try (Statement statement = connection.createStatement()) {
            // get all site_id and distinct user_id count for the given country grouped by site_id
            String query = "SELECT site_id, COUNT(distinct user_id) AS user_count FROM " + tableName + " WHERE country_id = \"" + country + "\" GROUP BY site_id;";
            ResultSet resultSet = statement.executeQuery(query);
            while (resultSet.next()) {
                String site = resultSet.getString("site_id");
                int users = resultSet.getInt("user_count");
                siteUserCountMap.put(site, users);
            }

            // get the site_id with max number of unique users from the retrieved data
            int maxCount = Collections.max(siteUserCountMap.values());
            for (String site : siteUserCountMap.keySet()) {
                if (siteUserCountMap.get(site) == maxCount) {
                    maxSiteId = site;
                }
            }

            // for this site_id with max count of unique users from the given country, get all unique user_id
            query = "SELECT DISTINCT user_id FROM " + tableName + " WHERE site_id = \"" + maxSiteId + "\" AND country_id = \"" + country + "\";";
            ResultSet userSet = statement.executeQuery(query);
            while (userSet.next()) {
                String userID = userSet.getString("user_id");
                numberOfUsers.add(userID);
            }

            result.put(maxSiteId, numberOfUsers.size());
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return result;
    }

    /**
     * Between the given timestamps, computes the who visited certain sites above the given threshold
     *
     * @param connection is the connection object with live connection to MySQL database
     * @param tableName  is name of the table
     * @param startTime  is the given start time
     * @param endTime    is the given end time
     * @param threshold  is the bound above which we need to consider our entries
     * @return the mapping of user_id to set of site_id and the number of sites which the user_id has visited more than
     * threshold times in the given timestamp window
     */
    private static Map<String, Pair<Set<String>, Integer>> getUsersAndSitesBetweenTimestamps(Connection connection, String tableName, String startTime, String endTime, String threshold) {
        Map<String, Pair<Set<String>, Integer>> result = new HashMap<>();
        try (Statement statement = connection.createStatement()) {
            // get all user_id between the timestamp who have visited certain websites more than 10 times
            String query = "SELECT user_id FROM (SELECT user_id, COUNT(site_id) AS visit FROM " + tableName + " WHERE ts BETWEEN \"" + startTime + "\" AND \"" + endTime + "\" group by user_id) as newTab where newTab.visit > " + threshold + ";";
            ResultSet users = statement.executeQuery(query);
            while (users.next()) {
                String user = users.getString("user_id");
                result.put(user, new Pair<>(new HashSet<>(), 0));
            }

            // for each such user_id, get the site_id which they have visited between the given start and end timestamps
            for (String userId : result.keySet()) {
                query = "SELECT site_id FROM " + tableName + " WHERE ts BETWEEN \"" + startTime + "\" AND \"" + endTime + "\" AND user_id = \"" + userId + "\";";
                ResultSet sites = statement.executeQuery(query);
                while (sites.next()) {
                    String site = sites.getString("site_id");
                    Set<String> userSites = result.get(userId).getKey();
                    userSites.add(site);
                    result.put(userId, new Pair<>(userSites, userSites.size()));
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return result;
    }

    /**
     * Computes the number of unique users for each site_id who last visited the site_id
     *
     * @param connection is the connection object with live connection to MySQL database
     * @param tableName  is name of the table
     * @return a hash map with each site_id corresponding to the number of unique user_id who have their last visit as
     * the site_id
     */
    private static Map<String, Integer> getUniqueNumberOfUsersPerSiteWithLatestVisit(Connection connection, String tableName) {
        Map<String, Integer> result = new HashMap<>();
        Map<String, Integer> sortedResult = new HashMap<>();
        try (Statement statement = connection.createStatement()) {
            // get all unique site_id and populate it in the site_id to latest visiting unique users
            String query = "SELECT DISTINCT site_id FROM " + tableName + ";";
            ResultSet sites = statement.executeQuery(query);
            while (sites.next()) {
                String site = sites.getString("site_id");
                result.put(site, 0);
            }

            for (String site : result.keySet()) {
                // get user_id with latest visit to this site
                query = "SELECT count(user_id) AS total_users FROM data_table AS a WHERE ts = (SELECT MAX(ts) FROM data_table AS b WHERE a.user_id = b.user_id) AND site_id = \"" + site + "\" GROUP BY site_id;";
                ResultSet userCount = statement.executeQuery(query);
                while (userCount.next()) {
                    int count = userCount.getInt("total_users");
                    result.put(site, count);
                }

                // sort the site_id to unique user_id visiting latest to the corresponding site_id hash map in
                // descending order
                sortedResult = result.entrySet()
                        .stream()
                        .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
                        .collect(
                                toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e2,
                                        LinkedHashMap::new));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return sortedResult;
    }

    /**
     * Computes and stores for each user the first and the last website they visited
     *
     * @param connection is the connection object with live connection to MySQL database
     * @param tableName  is name of the table
     * @return the number of users whose first/last visits are the same website
     */
    private static int getUsersWhoseFirstAndLastVisitedWebsiteAreSame(Connection connection, String tableName) {
        Map<String, String> firstVisit = new HashMap<>();
        Map<String, String> lastVisit = new HashMap<>();
        int count = 0;
        try (Statement statement = connection.createStatement()) {
            String query = "SELECT DISTINCT user_id FROM " + tableName + ";";
            ResultSet users = statement.executeQuery(query);
            while (users.next()) {
                String userId = users.getString("user_id");
                firstVisit.put(userId, "");
                lastVisit.put(userId, "");
            }
            for (String userId : firstVisit.keySet()) {
                // populate map with first visited website for each user
                String minQuery = "SELECT site_id FROM " + tableName + " AS a WHERE ts = (SELECT MIN(ts) FROM " + tableName + " AS b WHERE a.user_id = b.user_id) AND user_id = \"" + userId + "\";";
                ResultSet firstVisitSite = statement.executeQuery(minQuery);
                while (firstVisitSite.next()) {
                    String site = firstVisitSite.getString("site_id");
                    firstVisit.put(userId, site);
                }
                // populate map with last visited website for each user
                String maxQuery = "SELECT site_id FROM " + tableName + " AS a WHERE ts = (SELECT MAX(ts) FROM " + tableName + " AS b WHERE a.user_id = b.user_id) AND user_id = \"" + userId + "\";";
                ResultSet lastVisitSite = statement.executeQuery(maxQuery);
                while (lastVisitSite.next()) {
                    String site = lastVisitSite.getString("site_id");
                    lastVisit.put(userId, site);
                }

                // compute number of users whose first and last visits are to the same website
                if (firstVisit.get(userId).equals(lastVisit.get(userId))) {
                    count++;
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return count;
    }

    public static void main(String[] args) {
        File file = new File("src/data3.csv");
        String csvFile = file.getAbsolutePath();
        List<String> records = readFileFromSource(csvFile);

        Connection connection = connectToDB();
        String tableName = "data_table";
        createTable(connection, tableName);
        insertValuesIntoTable(connection, records);

        System.out.println("\n---------------- ANALYTICS ---------------\n");
        // Q1. Consider only the rows with country_id = "BDV" (there are 844 such rows). For each site_id, we can
        // compute the number of unique user_id's found in these 844 rows. Which site_id has the largest number of
        // unique users? And what's the number?
        System.out.println("The site with the most number of unique user_ids from the country BDV : ");
        System.out.println(getSiteWithLargestUsersInCountry(connection, tableName, "BDV") + "\n");

        // Q2. Between 2019-02-03 00:00:00 and 2019-02-04 23:59:59, there are four users who visited a certain site
        // more than 10 times. Find these four users & which sites they (each) visited more than 10 times.
        // (Simply provides four triples in the form (user_id, site_id, number of visits) in the box below.)
        System.out.println("The mapping of each user_id to the site_id and number of times he/she visited the site " +
                "between 2019-02-03 00:00:00 and 2019-02-04 23:59:59 : ");
        System.out.println(getUsersAndSitesBetweenTimestamps(connection, tableName, "2019-02-03 00:00:00",
                "2019-02-04 23:59:59", "10") + "\n");

        // Q3. For each site, compute the unique number of users whose last visit (found in the original data set) was
        // to that site. For instance, user "LC3561"'s last visit is to "N0OTG" based on timestamp data. Based on this
        // measure, what are top three sites? (hint: site "3POLC" is ranked at 5th with 28 users whose last visit in the
        // data set was to 3POLC; simply provide three pairs in the form (site_id, number of users).)
        System.out.println("The mapping for each site_id and number of unique users who last visited that site_id : ");
        System.out.println(getUniqueNumberOfUsersPerSiteWithLatestVisit(connection, tableName) + "\n");

        // Q4. For each user, determine the first site he/she visited and the last site he/she visited based on the
        // timestamp data. Compute the number of users whose first/last visits are to the same website. What is the
        // number?
        System.out.println("The number of user_id who have first/last visits to the same website : ");
        System.out.println(getUsersWhoseFirstAndLastVisitedWebsiteAreSame(connection, tableName));
    }
}
