package test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.QueryTrace;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.google.common.base.Stopwatch;

/**
 * Usage: java bulkload.BulkLoad
 */
public class CassandraTester {

    public static final String CASSANDRA_SERVER = "10.10.3.57";

    /** Keyspace name */
    public static final String KEYSPACE = "test";

    public static final Random random = new Random(0);

    private static final Map<String, PreparedStatement> cachedStatements =
        new HashMap<>();

    public static void main(String[] args) {
        if (args.length != 4) {
            System.err.println("usage: java " + CassandraTester.class.getName()
                + " tableprefix no_years no_pnodes no_prices");
            return;
        }

        String tablePrefix = args[0];
        int yearsNb = Integer.parseInt(args[1]);
        int pnodesNb = Integer.parseInt(args[2]);
        int pricesNb = Integer.parseInt(args[3]);

        final String tableName = tablePrefix + "_" + yearsNb;
        Cluster cluster = Cluster.builder().addContactPoints(CASSANDRA_SERVER)
            .withProtocolVersion(ProtocolVersion.NEWEST_SUPPORTED).build();

        // CREATE DATA
        try (Session session = cluster.connect()) {
            executeStatement(session, "DROP TABLE IF EXISTS " + KEYSPACE + "."
                + tableName);
            executeStatement(session, createSchema(tableName, pricesNb));

            Collection<String> names = generateNames(pnodesNb);
            SortedSet<Date> dates = generateDates(yearsNb);

            Stopwatch w = Stopwatch.createStarted();
            for (String name : names) {
                System.err.println("Inserting values for " + name + "...");
                for (Date date : dates) {
                    List<Object> values = new ArrayList<>();

                    values.add(name);
                    values.add(date);
                    values.addAll(generatePrices(pricesNb));

                    executeStatement(session, createInsertStatement(tableName,
                        pricesNb), values.toArray());
                }
            }
            System.err.println("Data created in " + w);

        } catch (Exception e) {
            e.printStackTrace();
        }

        // QUERY DATA
        try (Session session = cluster.connect()) {
            Stopwatch w = Stopwatch.createStarted();
            ResultSet rs = executeStatement(session, "SELECT * FROM " + KEYSPACE
                + "." + tableName);

            System.err.println("Select query returned + " + rs.all().size()
                + " rows");
            System.err.println(getStats(rs));
            System.err.println("Data requested in " + w);

        } catch (Exception e) {
            e.printStackTrace();
        }

        cluster.close();
        System.err.println("Done.");
    }

    private static ResultSet executeStatement(Session session, String statement,
        Object... params) {

        PreparedStatement st = cachedStatements.get(statement);
        if (st == null) {
            st = session.prepare(statement).enableTracing();
            cachedStatements.put(statement, st);
        }

        BoundStatement boundStatement = new BoundStatement(st);
        return session.execute(boundStatement.bind(params));
    }

    private static String getStats(ResultSet results) {
        final SimpleDateFormat format = new SimpleDateFormat("HH:mm:ss.SSS");
        ExecutionInfo executionInfo = results.getExecutionInfo();

        StringBuilder buffer = new StringBuilder();
        QueryTrace queryTrace = executionInfo.getQueryTrace();
        long lastEvent = 0;
        for (QueryTrace.Event event : queryTrace.getEvents()) {
            long elapsed = (event.getSourceElapsedMicros() / 1000) - lastEvent;
            lastEvent = elapsed;

            buffer.append(String.format("%78s | %12s | %10s | %12s | %12s\n",
                event.getDescription(), format.format(event.getTimestamp()),
                event.getSource(), event.getSourceElapsedMicros(), Duration.of(
                    event.getSourceElapsedMicros(), ChronoUnit.MICROS)
                    .toString()));

        }

        buffer.append(String.format("Host (queried): %s", executionInfo
            .getQueriedHost().toString()));

        return buffer.toString();
    }

    public static List<BigDecimal> generatePrices(int noPrice) {
        List<BigDecimal> prices = new ArrayList<>(noPrice);
        for (int i = 0; i < noPrice; i++) {
            prices.add(new BigDecimal(random.nextFloat() * 1000));
        }

        return prices;
    }

    public static Collection<String> generateNames(int noNames) {
        List<String> names = new ArrayList<>(noNames);
        for (int i = 0; i < noNames; i++) {
            names.add(new BigInteger(32, random).toString(32));
        }

        System.err.println("Generated names : " + Arrays.toString(names.toArray(
            new String[0])));

        return names;
    }

    public static SortedSet<Date> generateDates(int noYears) {
        ZonedDateTime endDate = ZonedDateTime.now();
        ZonedDateTime startDate = endDate.minusYears(noYears);

        SortedSet<Date> dateSet = new TreeSet<>();

        ZonedDateTime curDate = startDate;
        while (curDate.isBefore(endDate)) {
            dateSet.add(Date.from(curDate.toInstant()));
            curDate = curDate.plusHours(1);
        }

        System.err.println("Generated " + dateSet.size()
            + " hourly dates starting from  " + startDate + " (" + noYears
            + " years)");

        return dateSet;
    }

    /**
     * Schema for bulk loading table.
     * It is important not to forget adding keyspace name before table name,
     * otherwise CQLSSTableWriter throws exception.
     */
    public static String createSchema(String tblName, int noPrices) {
        String schema = "CREATE TABLE " + KEYSPACE + "." + tblName + " ("
            + "name ascii, date timestamp, ";
        for (int i = 0; i < noPrices; i++) {
            schema += "price" + i + " decimal";
            if (i + 1 < noPrices)
                schema += ", ";
        }
        schema += ", PRIMARY KEY (name, date) )";

        System.err.println("Creating schema:\n" + schema);

        return schema;
    }

    public static String createInsertStatement(String tblName, int noPrices) {
        String stmt = "INSERT INTO " + KEYSPACE + "." + tblName
            + " (name, date, ";

        for (int i = 0; i < noPrices; i++) {
            stmt += "price" + i;
            if (i + 1 < noPrices)
                stmt += ", ";
        }

        stmt += ") VALUES (?, ?, ";
        for (int i = 0; i < noPrices; i++) {
            stmt += "?";
            if (i + 1 < noPrices)
                stmt += ", ";
        }

        stmt += ")";

        return stmt;
    }
}
