import com.github.javafaker.Faker;
import org.jooq.*;
import org.jooq.impl.DSL;

import java.sql.*;
import java.sql.Statement;
import java.util.Random;

public class H2dbTest {
    // JDBC driver name and database URL
    static final String JDBC_DRIVER = "org.h2.Driver";
    static final String DB_URL = "jdbc:h2:mem:test";

    //  Database credentials
    static final String USER = "sa";
    static final String PASS = "";

    static long time;


    public static void main(String[] args) {
        Connection conn = null;
        Statement stmt = null;
        try {
            //Register JDBC driver
            Class.forName(JDBC_DRIVER);

            //Open a connection
            System.out.println("Connecting to database...");
            conn = DriverManager.getConnection(DB_URL,USER,PASS);

            //Create table
            System.out.println("Creating table in given database...");
            stmt = conn.createStatement();
            String sql =  "CREATE TABLE book " +
                    "(id INTEGER not NULL, " +
                    " author_id INTEGER, " +
                    " title VARCHAR(255), " +
                    " PRIMARY KEY ( id ))";
            stmt.executeUpdate(sql);

            sql = "CREATE TABLE author " +
                    "( author_id INTEGER, " +
                    " first_name VARCHAR(100), " +
                    " last_name VARCHAR(100)," +
                    " PRIMARY KEY ( author_id ))";
            stmt.executeUpdate(sql);

            System.out.println("Created tables in given database...");


            //Inserting data to table book with javafaker
            Faker faker = new Faker();

            Random random = new Random();
            for (int i = 1; i < 1000; i++) {
                sql = "INSERT INTO book " + "VALUES (" + i + "," + (random.nextInt(325) + 1) + ", \'" + faker.book().title().replaceAll("\'", " ") + "\')";
                stmt.executeUpdate(sql);
            }

            //Inserting data to author table
            sql = "SELECT DISTINCT author_id FROM book";
            ResultSet rs = stmt.executeQuery(sql);
            while (rs.next()){
                int author_id = rs.getInt("author_id");

                sql = "INSERT INTO author " + "VALUES (" + author_id + ", " + "\'" + faker.name().firstName().replaceAll("\'", " ") + "\'," + "\'" + faker.name().firstName().replaceAll("\'", " ") + "\')";
                conn.createStatement().executeUpdate(sql);
            }



            //Extract data from table with JDBC
            sql = "SELECT book.id, book.title, author.first_name, author.last_name " +
                    "FROM book INNER JOIN author ON book.author_id = author.author_id";
            time = System.currentTimeMillis();
            rs = stmt.executeQuery(sql);
            System.out.println("sql: " + (System.currentTimeMillis() - time));

            /*while(rs.next()) {
                int id  = rs.getInt("id");
                String title = rs.getString("title");
                String first_name = rs.getString("first_name");
                String last_name = rs.getString("last_name");


                System.out.print("ID: " + id);
                System.out.print(", Name: " + first_name + " " + last_name);
                System.out.println(", Title: " + title);
            }*/


            //Extract data with JOOQ
            DSLContext create = DSL.using(conn, SQLDialect.H2);
            time = System.currentTimeMillis();
            Result<Record> result = create.fetch(sql);
            System.out.println("jooq: " + (System.currentTimeMillis() - time));

           /* for (Record r : result) {
                System.out.println(r.toString());

           }*/


            stmt.close();
            conn.close();
        } catch(SQLException se) {
            se.printStackTrace();
        } catch(Exception e) {
            e.printStackTrace();
        } finally {

            try{
                if(stmt!=null) stmt.close();
            } catch(SQLException se2) {
            }
            try {
                if(conn!=null) conn.close();
            } catch(SQLException se){
                se.printStackTrace();
            }
        }
    }
}