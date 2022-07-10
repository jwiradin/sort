package com.test;

import org.apache.log4j.*;
import org.json.JSONException;
import org.json.JSONObject;

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

    private static String filter;

    public static void main(String[] args) {
        Logger root = Logger.getRootLogger();
        root.addAppender(new ConsoleAppender(new PatternLayout("%d %p (%t) [%c] - %m%n")));
        root.setLevel(Level.INFO);

        for (String a : args) {
            if (a.startsWith("-p")) {
                path = a.substring(3);
            } else if (a.startsWith("-d")) {
                root.setLevel(Level.DEBUG);
            } else if (a.startsWith("-f")) {
                filter = a.substring(3);
            }
        }
        if ("".equals(path)) {
            System.out.println("argument -p=path to log folder is required");
            return;
        }

        log = Logger.getLogger(Sort.class);

        try {

            log.info("Starting ----");
            Connection dbConnection = prepareDB();
            Process(dbConnection, path, filter);
            dbConnection.close();
            log.info("Completed ----");
            //System.out.println(dtFormatter.format(LocalDateTime.now()) + " Completed ----");

        } catch (Exception ex) {
            log.error(ex);
        }

    }

    private static void Process(Connection dbConnection, String path, String filter) {
        String sql;

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

                    //if (!extract(dbConnection, file)) {
                    processFile(dbConnection, file);
                    //}

                    if (filter == null) {
                        sql = "Select id, data from data order by id";
                    } else {
                        sql = "Select d.id, d.data from data d where d.data ilike '%filter%' order by d.id";
                    }

                    rs = dbConnection.prepareStatement("select count(*) from data").executeQuery();
                    if (rs.next()) log.debug(String.format("%04d", rs.getInt(1)));

                    rs = dbConnection.prepareStatement(sql).executeQuery();

                    BufferedWriter writer = new BufferedWriter(new FileWriter(output.getAbsolutePath()));
                    log.debug("Writing to file");

                    while (rs.next()) {
                        writer.write(rs.getString("data"));
                    }
                    writer.close();
                    log.info("End processing file: " + file.getAbsolutePath());

                } else {
                    log.info("File: " + file.getAbsolutePath() + " has been previously sorted.");
                }
            }

        } catch (Exception ex) {
            System.out.println(dtFormatter.format(LocalDateTime.now()) + " " + ex.getMessage());
        }
    }
/*
    private static boolean extract(Connection dbConnection, File file) {
        log.debug("Extract");
        String ln;
        try {
            Scanner sc = new Scanner(file);
            if (sc.hasNext()) {
                ln = sc.nextLine();
                sc.close();

                if (ln.indexOf("{\"message\":") != 25) {
                    return false;
                } else {

                    StringBuilder sb = new StringBuilder();
                    Pattern ex = Pattern.compile("([0-9]{4}-[0-9]{2}-[0-9]{2}\\W[0-2][0-9]:[0-5][0-9]:[0-5][0-9],[0-9]{3}\\W)");
                    String tmp = "";
                    String prvKey = "";
                    Long seq = Long.parseLong("0");
                    JSONObject json = null;
                    try {
                        PreparedStatement ps = dbConnection.prepareStatement("truncate table data");
                        ps.executeUpdate();
                        ps.close();
                        sc = new Scanner(file);
                        ps = dbConnection.prepareStatement("insert into data (id,data) values(?,?)");

                        Matcher m;
                        while (sc.hasNext()) {
                            json = new JSONObject(sc.nextLine().substring(25));

                            if (json.has("message")) {
                                tmp = json.get("message").toString();

                                m = ex.matcher(tmp);
                                if (m.find()) {
                                    prvKey = m.group(0) + String.format("%06d", seq);
                                    sb.append(tmp + "\n");

                                    while (sc.hasNext()) {
                                        json = new JSONObject(sc.nextLine().substring(25));
                                        if (json.has("message")) {
                                            tmp = json.get("message").toString();
                                            m = ex.matcher(tmp);
                                            if (m.find()) {
                                                ps.setString(1, prvKey);
                                                ps.setString(2, sb.toString());
                                                ps.executeUpdate();

                                                seq++;
                                                prvKey = m.group(0) + String.format("%06d", seq);
                                                sb.setLength(0);
                                            }
                                            sb.append(tmp + "\n");
                                        }
                                    }
                                } else {
                                    log.debug("Skipping:" + tmp);
                                }

                            } else {
                                log.debug(json.toString());
                            }
                        }
                        if (sb.length() > 0) {
                            ps.setString(1, prvKey);
                            ps.setString(2, sb.toString());
                            ps.executeUpdate();
                        }
                        ps.close();
                    } catch (StringIndexOutOfBoundsException e) {
                        log.error(e);
                    } catch (SQLException e) {
                        log.error(e);
                    } catch (JSONException e) {

                        log.debug(json.toString());
                        log.error(e);
                    }

                }
            }
        } catch (FileNotFoundException e) {
            log.error(e);
        }
        return true;
    }

    private static void processFile(Connection dbConnection, File file) {

        StringBuilder sb = new StringBuilder();
        Pattern ex = Pattern.compile("([0-9]{4}-[0-9]{2}-[0-9]{2}\\W[0-2][0-9]:[0-5][0-9]:[0-5][0-9],[0-9]{3}\\W)");
        String tmp = "";
        String prvKey = "";
        Long seq = Long.parseLong("0");

        try {
            PreparedStatement ps = dbConnection.prepareStatement("truncate table data");
            ps.executeUpdate();
            ps.close();

            ps = dbConnection.prepareStatement("insert into data (id,data) values(?,?)");

            Scanner sc = new Scanner(file);
            Matcher m;
            while (sc.hasNext()) {
                tmp = sc.nextLine();
                m = ex.matcher(tmp);
                if (m.find()) {
                    prvKey = m.group(0) + String.format("%06d", seq);
                    sb.append(tmp + "\n");

                    while (sc.hasNext()) {
                        tmp = sc.nextLine();
                        m = ex.matcher(tmp);
                        if (m.find()) {
                            ps.setString(1, prvKey);
                            ps.setString(2, sb.toString());
                            ps.executeUpdate();

                            seq++;
                            prvKey = m.group(0) + String.format("%06d", seq);
                            sb.setLength(0);
                        }
                        sb.append(tmp + "\n");
                    }
                } else {
                    log.debug("Skipping:" + tmp);
                }
            }
            if (sb.length() > 0) {
                ps.setString(1, prvKey);
                ps.setString(2, sb.toString());
                ps.executeUpdate();
            }
            ps.close();
        } catch (StringIndexOutOfBoundsException e) {
            log.error(e);
        } catch (SQLException e) {
            log.error(e);
        } catch (FileNotFoundException e) {
            log.error(e);
        }
    }
*/
    private static void processFile(Connection dbConnection, File file) {

        StringBuilder sb = new StringBuilder();
        Pattern ex = Pattern.compile("([0-9]{4}-[0-9]{2}-[0-9]{2}\\W[0-2][0-9]:[0-5][0-9]:[0-5][0-9],[0-9]{3}\\W)");
        String tmp = "";
        String prvKey = "";
        Long seq = Long.parseLong("0");
        boolean isJSON = false;
        JSONObject json;

        try {

            Scanner sc = new Scanner(file);
            if (sc.hasNext()) {
                tmp = sc.nextLine();
                sc.close();

                if (tmp.indexOf("{\"message\":") != 25) {
                    isJSON = true;
                }
            }

            PreparedStatement ps = dbConnection.prepareStatement("truncate table data");
            ps.executeUpdate();
            ps.close();

            ps = dbConnection.prepareStatement("insert into data (id,data) values(?,?)");

            sc = new Scanner(file);
            Matcher m;
            while (sc.hasNext()) {
                tmp = sc.nextLine();

                if (isJSON) {
                    json = new JSONObject(tmp.substring(25));

                    if (json.has("message")) {
                        tmp = json.get("message").toString();
                    }
                    else {
                        continue;
                    }
                }

                m = ex.matcher(tmp);
                if (m.find()) {
                    prvKey = m.group(0) + String.format("%06d", seq);
                    sb.append(tmp + "\n");

                    while (sc.hasNext()) {
                        tmp = sc.nextLine();
                        m = ex.matcher(tmp);
                        if (m.find()) {
                            ps.setString(1, prvKey);
                            ps.setString(2, sb.toString());
                            ps.executeUpdate();

                            seq++;
                            prvKey = m.group(0) + String.format("%06d", seq);
                            sb.setLength(0);
                        }
                        sb.append(tmp + "\n");
                    }
                } else {
                    log.debug("Skipping:" + tmp);
                }
            }
            if (sb.length() > 0) {
                ps.setString(1, prvKey);
                ps.setString(2, sb.toString());
                ps.executeUpdate();
            }
            ps.close();
        } catch (StringIndexOutOfBoundsException e) {
            log.error(e);
        } catch (SQLException e) {
            log.error(e);
        } catch (FileNotFoundException e) {
            log.error(e);
        } catch (JSONException e) {
            log.error(e);
        }
    }

    private static Connection prepareDB() {
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

        return null;
    }
}
