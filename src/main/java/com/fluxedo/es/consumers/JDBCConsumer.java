package com.fluxedo.es.consumers;

import com.espertech.esper.client.EventBean;
import com.fluxedo.es.commons.Consumer;
import com.jsoniter.JsonIterator;
import com.jsoniter.any.Any;
import org.apache.commons.dbcp2.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Created by Marco Balduini on 18/06/2018 as part of project esperservices.
 */
public class JDBCConsumer extends Consumer {

    /*
    - passo i campi nella config
    - i nomi devono corrispondere a quelli dei risultati epl
    - i nomi devono essere dati nell'ordine della tabella sql
    - la tabella sql deve esistere già
    - i tipi nella config devono corrispondere alla tabella sql
     */

    private String driver;
    private String jdbcConnectionString;
    private String username;
    private String password;

    private String query;

    private BasicDataSource connectionPool = null;

    private Any dataConfig;

    private Logger logger = LoggerFactory.getLogger(JDBCConsumer.class);

    private Connection connection = null;
    private PreparedStatement preparedStatement = null;

    private boolean sampling = false;

    public JDBCConsumer() {
        super();
    }

    public void initialize(String configuration){
        try {

            Any config = JsonIterator.deserialize(configuration);
            Any jdbcConfig = config.get("jdbcConfig").get("connectionConfig");
            dataConfig = config.get("jdbcConfig").get("dataSchema");

            sampling = config.get("sampling").toBoolean();

            this.driver = jdbcConfig.get("driver").toString();
            this.jdbcConnectionString = jdbcConfig.get("jdbcConnectionString").toString();
            this.username = jdbcConfig.get("username").toString();
            this.password = jdbcConfig.get("password").toString();

            Class.forName(driver).newInstance();
            connectionPool = new BasicDataSource();

            connectionPool.setDriverClassName(driver);
            connectionPool.setUrl(jdbcConnectionString);
            connectionPool.setUsername(username);
            connectionPool.setPassword(password);
            connectionPool.setInitialSize(1);
            connectionPool.setMaxConnLifetimeMillis(60000);
            connectionPool.setMaxTotal(5);
            connectionPool.setDefaultQueryTimeout(5000);

//            query = "INSERT INTO " + config.get("jdbcConfig").get("dbName").toString() + "." + config.get("jdbcConfig").get("tableName").toString() + " (";

            query = "INSERT INTO " + config.get("jdbcConfig").get("tableName").toString() + " (";

            for(Any field : dataConfig.asList()){
                query = query + field.get("fieldName").toString() + ",";
            }

            query = query.substring(0, query.length() - 1) + ") VALUES (";

            for(int i = 0 ; i < dataConfig.asList().size() ; i++){
                query = query + "?,";
            }

            query = query.substring(0, query.length() - 1) + ")";

            try {
                connection = connectionPool.getConnection();
                connection.setAutoCommit(false);
                preparedStatement = connection.prepareStatement(query);
            } catch (SQLException e) {
                e.printStackTrace();
            }

        } catch (InstantiationException e) {
            logger.error("Exception while connecting to the database", e);
        } catch (IllegalAccessException e) {
            logger.error("Exception while connecting to the database", e);
        } catch (ClassNotFoundException e) {
            logger.error("Exception while connecting to the database", e);
        }
    }

    public void update(EventBean[] newData, EventBean[] oldData) {

        for (EventBean e : newData) {
            if (e != null) {
                try {
                    int i = 1;
                    for (Any field : dataConfig.asList()) {

                        switch (field.get("fieldType").toString().toLowerCase()) {
                            case "string":
                                preparedStatement.setString(
                                        i,
                                        e.get(field.get("fieldName").toString()).toString());
                                break;
                            case "int":
                                preparedStatement.setInt(
                                        i,
                                        Integer.parseInt(e.get(field.get("fieldName").toString()).toString()));
                                break;
                            case "double":
                                preparedStatement.setDouble(
                                        i,
                                        Double.parseDouble(e.get(field.get("fieldName").toString()).toString()));
                                break;
                            default:
                                preparedStatement.setString(
                                        i,
                                        e.get(field.get("fieldName").toString()).toString());
                                break;
                        }
                        i++;
                    }

                    preparedStatement.addBatch();

                } catch (Exception ex) {
                    logger.error("Exception while preparing the preparedStatement", ex);
                }
            }

            if(sampling)
                break;
        }
        try {
            int result[] = preparedStatement.executeBatch();
            connection.commit();
//            logger.info(result.length + " rows written on db");
        } catch (SQLException ex) {
            logger.error("Exception while executing the preparedStatement", ex);
        }
    }
}
