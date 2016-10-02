package com.nileshgule;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws IOException {
        Date start = new Date();

        SimpleDateFormat dateFormat = new SimpleDateFormat("dd MM YYYY hh:mm:ss");

        System.out.println( "Start time " + dateFormat.format(start) );

        Configuration config = HBaseConfiguration.create();

        String tableName = "employee_standard";

        Connection connection = null;
        Admin admin = null;
        Table table = null;

        try {
            System.out.println("Creating connection");
            connection = ConnectionFactory.createConnection(config);
            admin = connection.getAdmin();

            if (!admin.isTableAvailable(TableName.valueOf(tableName))) {
                System.out.println("Creating table definition for " + tableName);
                HTableDescriptor hbaseTable = new HTableDescriptor(TableName.valueOf(tableName));
                hbaseTable.addFamily(new HColumnDescriptor("name"));
                hbaseTable.addFamily(new HColumnDescriptor("contact_info"));
                hbaseTable.addFamily(new HColumnDescriptor("personal_info"));
                admin.createTable(hbaseTable);
            } else {
                System.out.println(tableName +" already exists");
            }
            table = connection.getTable(TableName.valueOf(tableName));

            System.out.println("Creating data for insertion");

            //    creating sample data that can be used to save into hbase table
            String[][] people = {
                    { "1", "Marcel", "Haddad", "marcel@xyz.com", "M", "26" },
                    { "2", "Franklin", "Holtz", "franklin@xyz.com", "M", "24" },
                    { "3", "Dwayne", "McKee", "dwayne@xyz.com", "M", "27" },
                    { "4", "Rae", "Schroeder", "rae@xyz.com", "F", "31" },
                    { "5", "Rosalie", "burton", "rosalie@xyz.com", "F", "25" },
                    { "6", "Gabriela", "Ingram", "gabriela@xyz.com", "F", "24" } };

            for (int i = 0; i < people.length; i++) {
                Put person = new Put(Bytes.toBytes(people[i][0]));
                person.addColumn(Bytes.toBytes("name"), Bytes.toBytes("first"), Bytes.toBytes(people[i][1]));
                person.addColumn(Bytes.toBytes("name"), Bytes.toBytes("last"), Bytes.toBytes(people[i][2]));
                person.addColumn(Bytes.toBytes("contact_info"), Bytes.toBytes("email"), Bytes.toBytes(people[i][3]));
                person.addColumn(Bytes.toBytes("personal_info"), Bytes.toBytes("gender"), Bytes.toBytes(people[i][4]));
                person.addColumn(Bytes.toBytes("personal_info"), Bytes.toBytes("age"), Bytes.toBytes(people[i][5]));
                table.put(person);
            }

            System.out.println("data inserted");

        }catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (admin != null) {
                    admin.close();
                }

                if (table != null) {
                    table.close();
                }

                if (connection != null && !connection.isClosed()) {
                    connection.close();
                }
            } catch (Exception e2) {
                e2.printStackTrace();
            }
        }

        System.out.println("Voila, success");

        Date end = new Date();

        System.out.println( "Start time " + dateFormat.format(end) );

        System.out.println("Total time taken : " + (end.getTime()-start.getTime())/1000 + " seconds");
    }
}
