package sjtu.edu.cn;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import org.neo4j.driver.v1.*;

import static java.util.Arrays.asList;

public class Insert {

    public static final String route_table[] = {
            "202.120.40.20:8281",
            "202.120.40.20:5481",
            "202.120.40.20:5381"};

    static List sessions;

    public static void main(String args[]) throws ParseException {
        List drivers = new ArrayList<Driver>();
        sessions = new ArrayList<Session>();
        List roles = new ArrayList<List>(); //存关系
        List nodes = new ArrayList<List>(); //存节点

        for (int i = 0; i < route_table.length; i++) {
            Driver driver = GraphDatabase.driver("bolt://" + route_table[i],
                    AuthTokens.basic("neo4j", ""));
            Session session = driver.session();
            drivers.add(driver);
            sessions.add(session);

            roles.add(new ArrayList<String>());
            nodes.add(new ArrayList<String>());
        }


        List role =
                asList(asList(2, 3, "2017-01-12", 1000), asList(1, 3, "2017-01-13", 1000)
                        , asList(1, 2, "2017-02-12", 1000), asList(2, 1, "2017-12-12", 1000));
        List node =
                asList(asList(2, "Seni"), asList(3, "Victor"));

        DateFormat format1 = new SimpleDateFormat("yyyy-MM-dd");
        Calendar cal = Calendar.getInstance();
        for (int i = 0; i < role.size(); i++) {
            List tmp = (List) role.get(i);
            String date = tmp.get(2).toString();
            cal.setTime(format1.parse(date));
            int month = cal.get(Calendar.MONTH);
            int index = (month - 1) / (12 / route_table.length);
            List tmp_list = (List) roles.get(index);
            tmp_list.add(role.get(i));
        }
        String insertNodeQuery = "UNWIND {pairs} as pair MERGE (p:Person{id:pair[0],name:pair[1]})";//as pair create n={id:pair[0], name:pair[1]}";

        String insertQuery = "UNWIND {pairs} as pair " +
                "MATCH (p1:Person {id:pair[0]}) " +
                ",(p2:Person {id:pair[1]}) " +
                "MERGE (p1)-[:SEND{date:pair[2],amount:pair[3]}]-(p2);";

        for (int i = 0; i < route_table.length; i++) {
            List tmp_data = (List) roles.get(i);
            Session s = (Session) sessions.get(i);
            //先插入所有节点
            s.run(insertNodeQuery, Collections.<String, Object>singletonMap("pairs", node)).consume();
            //再插入对应关系
            s.run(insertQuery, Collections.<String, Object>singletonMap("pairs", tmp_data)).consume();
        }

        System.out.println("Insert complete!!");

    }
}