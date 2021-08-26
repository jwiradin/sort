package com.test;

import org.apache.log4j.*;

import java.io.*;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class Sort {

    private static final String DB_DRIVER = "org.h2.Driver";
    private static final String DB_CONNECTION = "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1";
    private static final String DB_USER = "";
    private static final String DB_PASSWORD = "";

    private static final String CREATE_TABLE = "create table data(id varchar(30), data CLOB)";
    private static final String INSERT = "insert into data (id, data) values(?,?)";

    private static final DateTimeFormatter dtFormatter = DateTimeFormatter.ofPattern("YYYY-MM-dd HH:mm:ss,SSS");

    private static String path;

    private static Logger log;

    public static void main(String[] args) {
        Logger root = Logger.getRootLogger();
        root.addAppender(new ConsoleAppender(new PatternLayout("%d %p (%t) [%c] - %m%n")));
        root.setLevel(Level.INFO);

        for (String a: args) {
            if(a.startsWith("-p")){
                path = a.substring(3, args[0].length());
            }else if(a.startsWith("-d")){
                root.setLevel(Level.DEBUG);
            }
        }
        if ("".equals(path)){
            System.out.println("argument -p=path to log folder is required");
            return;
        }

        log = Logger.getLogger(Sort.class);

        try {

            log.info("Starting ----");
            Connection dbConnection = prepareDB();
            Process(dbConnection, path);
            dbConnection.close();
            log.info("Completed ----");
            //System.out.println(dtFormatter.format(LocalDateTime.now()) + " Completed ----");

        } catch (Exception ex) {
            log.error(ex);
        }

    }

    private static void Process(Connection dbConnection, String path) {

        FilenameFilter init = new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.toLowerCase().contains("pega") && !name.toLowerCase().contains("-sorted");
            }
        };

        //get all files
        try {
            File dir = new File(path);

            File files[] = dir.listFiles(init);

            ResultSet rs;

            for (File file : files) {

                File output = new File(file.getAbsolutePath() + "-sorted");

                if (!output.exists()) {

                    log.info("Start processing file: " + file.getAbsolutePath());
                    processFile(dbConnection, file);

                    rs = dbConnection.prepareStatement("Select data from data order by id").executeQuery();

                    BufferedWriter writer = new BufferedWriter(new FileWriter(output.getAbsolutePath()));
                    log.debug("Writing to file");
                    while(rs.next()){
                        writer.write(rs.getString("data"));
                    }
                    writer.close();
                    log.info("End processing file: " + file.getAbsolutePath());

                } else{
                    log.info("File: "+ file.getAbsolutePath() + " has been previously sorted.");
                }
            }

        } catch (Exception ex) {
            System.out.println(dtFormatter.format(LocalDateTime.now()) + " " + ex.getMessage());
        }
    }

    private static void processFile(Connection dbConnection, File file){

        StringBuilder sb = new StringBuilder();
        Pattern ex = Pattern.compile("([0-9]{4}-[0-9]{2}-[0-9]{2}\\W[0-2][0-9]:[0-5][0-9]:[0-5][0-9],[0-9]{3}\\W)");
        String tmp ="";
        String prvKey = "";
        Long seq = Long.parseLong("0");

        try {
            PreparedStatement ps = dbConnection.prepareStatement("truncate table data");
            ps.executeUpdate();
            ps.close();

            ps = dbConnection.prepareStatement("insert into data (id,data) values(?,?)");

            Scanner sc = new Scanner(file);
            Matcher m;
            while(sc.hasNext()) {
                tmp = sc.nextLine();
                m = ex.matcher(tmp);
                if (m.find()) {
                    prvKey = m.group(0) + String.format("000000", seq);
                    sb.append(tmp + "\n");

                    while (sc.hasNext()) {
                        tmp = sc.nextLine();
                        m = ex.matcher(tmp);
                        if (m.find()) {
                            ps.setString(1, prvKey);
                            ps.setString(2, sb.toString());
                            ps.executeUpdate();

                            seq++;
                            prvKey = m.group(0) + String.format("000000", seq);
                            sb.setLength(0);
                        }
                        sb.append(tmp + "\n");
                    }
                } else {
                    log.debug("Skipping:" + tmp);
                }
            }
            if(sb.length()>0) {
                ps.setString(1, prvKey);
                ps.setString(2, sb.toString());
                ps.executeUpdate();
            }
            ps.close();
        }
        catch (StringIndexOutOfBoundsException e){
            log.error(e);
        }
        catch (SQLException e){
            log.error(e);
        }
        catch(FileNotFoundException e){
            log.error(e);
        }
    }

    private static Connection prepareDB(){
        Connection dbConnection = null;

        try {
            log.debug("Starting in memory h2 server");
            Class.forName(DB_DRIVER);
            dbConnection = DriverManager.getConnection(DB_CONNECTION, DB_USER, DB_PASSWORD);

            log.debug("Creating table");
            PreparedStatement createTable = dbConnection.prepareStatement(CREATE_TABLE);
            createTable.executeUpdate();
            createTable.close();

            return dbConnection;
        } catch (SQLException e) {
            log.error(e);
        } catch (ClassNotFoundException e) {
            log.error(e);
        }

        return  null;
    }
}
