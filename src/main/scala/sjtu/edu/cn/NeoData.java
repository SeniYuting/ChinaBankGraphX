package sjtu.edu.cn;

import org.neo4j.driver.v1.*;
import org.neo4j.driver.v1.types.Node;
import org.neo4j.driver.v1.types.Relationship;

import java.util.*;

import static org.neo4j.driver.v1.Values.parameters;

public class NeoData {

    public static final String route_table[] = {
            "202.120.40.20:8281",
            "202.120.40.20:5481",
            "202.120.40.20:5381"};

    static Set<Node> nodes = new HashSet<Node>();
    static Set<Relationship> relationships = new HashSet<Relationship>();

    static {
        List drivers = new ArrayList<Driver>();
        List sessions = new ArrayList<Session>();

        for (int i = 0; i < route_table.length; i++) {
            Driver driver = GraphDatabase.driver("bolt://" + route_table[i],
                    AuthTokens.basic("neo4j", ""));
            Session session = driver.session();
            drivers.add(driver);
            sessions.add(session);

        }

        //返回结果
        StatementResult result;
        String roleQuery =
                " MATCH (p1:Person)-[r:SEND]-(p2:Person) " +
                        " WHERE r.date>={startDate} " +
                        "   AND r.date<={endDate} " +
                        " RETURN r as role, p1 as fromPerson, p2 as toPerson";
        Session s = (Session) sessions.get(0);
        result = s.run(roleQuery, parameters("startDate", "2017-01-01", "endDate", "2017-02-01"));

        while (result.hasNext()) {
            Record r = result.next();
            Node fp = r.get("fromPerson").asNode();
            org.neo4j.driver.v1.types.Relationship role = r.get("role").asRelationship();
            Node tp = r.get("toPerson").asNode();

            nodes.add(fp);
            nodes.add(tp);

            relationships.add(role);
        }

    }

    public static void main(String[] args) {
        System.out.println(NeoData.nodes.size());
        System.out.println(NeoData.relationships.size());
    }
}
