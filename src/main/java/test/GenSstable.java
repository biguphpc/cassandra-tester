package test;

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;

/**
 * Usage: java bulkload.BulkLoad
 */
public class GenSstable {

    /** Default output directory */
    public static final String DEFAULT_OUTPUT_DIR = System.getProperty(
        "java.io.tmpdir") + "/data";

    /** Keyspace name */
    public static final String KEYSPACE = "test";

    public static final Random random = new Random(0);

    public static void main(String[] args) {
        if (args.length != 4) {
            System.out.println(
                "usage: java test.gendata tablename no_years no_pnodes no_prices");
            return;
        }

        String tblname = args[0];
        int yearsNb = Integer.parseInt(args[1]);
        int pnodesNb = Integer.parseInt(args[2]);
        int pricesNb = Integer.parseInt(args[3]);

        // magic!
        Config.setClientMode(true);

        String dir = DEFAULT_OUTPUT_DIR;
        // Create output directory that has keyspace and table name in the path
        File outputDir = new File(dir + File.separator + KEYSPACE
            + File.separator + tblname);
        if (!outputDir.exists() && !outputDir.mkdirs()) {
            throw new RuntimeException("Cannot create output directory: "
                + outputDir);
        }

        // Prepare SSTable writer
        CQLSSTableWriter.Builder builder = CQLSSTableWriter.builder();
        // set output directory
        builder.inDirectory(outputDir)
            // set target schema
            .forTable(getSchema(tblname, pricesNb))
            // set CQL statement to put data
            .using(createInsertStatement(tblname, pricesNb))
            // set partitioner if needed
            // default is Murmur3Partitioner so set if you use different one.
            .withPartitioner(new Murmur3Partitioner());
        CQLSSTableWriter writer = builder.build();

        Collection<String> names = generateNames(pnodesNb);
        SortedSet<Date> dates = generateDates(yearsNb);

        try {
            for (String name : names) {
                System.out.println("Inserting values for " + name + "...");
                for (Date date : dates) {
                    List<Object> values = new ArrayList<>();

                    values.add(name);
                    values.add(date);
                    values.addAll(generatePrices(pricesNb));

                    writer.addRow(values);
                }
            }
        } catch (IOException | InvalidRequestException e) {
            e.printStackTrace();
        } finally {
            try {
                writer.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        System.out.println("Done generating sstable.");
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

        System.out.println("Created names : " + Arrays.toString(names.toArray(
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

        System.out.println("Created " + dateSet.size()
            + " hourly dates starting from  " + startDate + " (" + noYears
            + " years)");

        return dateSet;
    }

    /**
     * Schema for bulk loading table.
     * It is important not to forget adding keyspace name before table name,
     * otherwise CQLSSTableWriter throws exception.
     */
    public static String getSchema(String tblName, int noPrices) {
        String schema = "CREATE TABLE " + KEYSPACE + "." + tblName + " ("
            + "name ascii, date timestamp, ";
        for (int i = 0; i < noPrices; i++) {
            schema += "price" + i + " decimal";
            if (i + 1 < noPrices)
                schema += ", ";
        }
        schema += ", PRIMARY KEY (name, date) )";

        System.out.println("Creating schema:\n" + schema);

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
