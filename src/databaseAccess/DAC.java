package databaseAccess;

import databaseAccess.mapping.Ignore;
import databaseAccess.mapping.SerialPrimaryKey;
import databaseAccess.utils.Misc;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.*;
import java.util.ArrayList;

import static databaseAccess.utils.Misc.convertForSql;
import static databaseAccess.utils.Misc.toUpperFirstChar;

// Database access connection (for connection)
class DAC {
    private static Connection connection = null;
    private static final String DriverClassName = "org.postgresql.Driver";
    private static final String url = "jdbc:postgresql://localhost:5432/garagerodolphe?user=postgres&password=pixel";

    /**
     * Create connection for the jdbc identified by
     * the static attributes of da.DAC Class
     * @param AutoCommit defined if the connection returned is
     * setted as autocommit or not
     * @return a connection
     */
    private static Connection CreateConnection(boolean AutoCommit) throws ClassNotFoundException, SQLException {
        Class.forName(DriverClassName);
        Connection con = DriverManager.getConnection(url);
        con.setAutoCommit(AutoCommit);
        return con;
    }
    /**
     * Get the connection defined with da.DAC url and driver
     * if there is no connection or the actual connection is closed
     * it will create one
     * @return a connection
     */
    public static Connection getConnection() throws SQLException, ClassNotFoundException {
        if(connection == null || connection.isClosed())
            setConnection(CreateConnection(false));
        return connection;
    }

    /**
     * Get all the object's fields that are used in the database table
     * @param object the object that we want to get the fields
     * @return a List of those fields
     */
    public static ArrayList<Field> getDAFields(Object object){
        Field[] fields = object.getClass().getDeclaredFields();
        ArrayList<Field> val = new ArrayList<>();
        for (int i = 0; i < fields.length; i++)
            if(!fields[i].isAnnotationPresent(Ignore.class))
                val.add(fields[i]);
        return val;
    }

    /**
     * Get all the object's fields that are used in the database table
     * @param con connection that will be used
     * @param retour the return of the sql query
     * oh: select <span style="text-decoration: underlined">retour</span> from table
     * @param table the relation
     * oh: select retour from <span style="text-decoration: underlined">table</span>
     * @param predicat the condition
     * oh: select retour from table where <span style="text-decoration: underlined">predicat</span>
     * @return the value of the first column as String
     */
    public static String getFirstColumnValue(Connection con, String retour, String table, String predicat) throws SQLException {
        Statement st = con.createStatement();
        if(!predicat.trim().equals(""))
            predicat = "WHERE "+predicat;
        String query = "SELECT "+ retour +" FROM " + table + " " + predicat;
        ResultSet rs = st.executeQuery(query);
        rs.next();
        String val = rs.getString(1);
        return val;
    }

    /**
     * Get the all the rows value of the specified column from the results of a SQL query.
     * @param con Connection object to the database
     * @param column The name of the column whose values need to be retrieved.
     * @param query SQL query to be executed
     * @return ArrayList containing the values of the specified column
     * @throws SQLException If there is an error with the SQL query or database connection
     */
    public static ArrayList<String> getColumnRow(Connection con, String column, String query) throws SQLException {
        Statement st = con.createStatement();
        ResultSet rs = st.executeQuery(query);
        ArrayList<String> val = new ArrayList<>();
        while(rs.next())
            val.add(rs.getString(column));
        rs.close(); st.close();
        return val;
    }


    public static String getFieldTypeName(Class<?> fieldType){
        String fieldTypeName = null;
        String[] SplittedfieldTypeName = fieldType.getTypeName().split("\\.");
        try{
            fieldTypeName = SplittedfieldTypeName[SplittedfieldTypeName.length-1];
        }catch (Exception e){
            fieldTypeName = fieldType.getTypeName();
        }
        return fieldTypeName;
    }

    /**
     * Create an ArrayList from the results of a SQL query
     * @param obj   object that will be used to create the arraylist
     * @param rs    is the resultset
     * @return      ArrayList containing the results of the query
     */
    public static ArrayList sqltoArray(Object obj, ResultSet rs) throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException, SQLException {
        ArrayList<Field> fields = getDAFields(obj);
        ArrayList val = new ArrayList();
        while(rs.next()){
            Object object = obj.getClass().getConstructor().newInstance();
            for (Field field : fields) {
                Class<?> fieldType = field.getType();
                String fieldTypeName = getFieldTypeName(field.getType());
                String fieldName = field.getName();
                Method getArg = rs.getClass().getMethod("get"+toUpperFirstChar(fieldTypeName) , String.class);
                getArg.setAccessible(true);
                Object getRsField = getArg.invoke(rs,fieldName);
                Method setField=object.getClass().getMethod("set"+toUpperFirstChar(fieldName), fieldType);
                setField.setAccessible(true);
                setField.invoke(object, getRsField);
            }
            val.add(object);
            object = null;
        }
        return val;
    }

    /**
     * Create an ArrayList from the results of a SQL query
     * @param con  Connection object to the database
     * @param query SQL query to be executed
     * @param obj  object that will be used to create the arraylist
     * @return ArrayList containing the results of the query
     * @throws SQLException If there is an error with the SQL query or database connection
     * @throws InvocationTargetException if the underlying method throws a da.exception.
     * @throws NoSuchMethodException if a matching method is not found.
     * @throws InstantiationException if this Class represents an abstract class, an interface, an array class, a primitive type, or void; or if the class has no nullary constructor; or if the instantiation fails for some other reason.
     * @throws IllegalAccessException if this Class represents an abstract class, an interface, an array class, a primitive type, or void; or if the class has no nullary constructor; or if the instantiation fails for some other reason.
     */
    public static ArrayList sqltoArray(Connection con, String query, Object obj) throws SQLException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        Statement st = con.createStatement();
        ResultSet rs = st.executeQuery(query);
        ArrayList val = sqltoArray(obj, rs);
        rs.close();
        st.close();
        return val;
    }

    public static String[] getFieldsName(Object obj){
        ArrayList<Field> fields = getDAFields(obj);
        String[] val = new String[fields.size()];
        for (int i = 0; i < val.length; i++)
            val[i] = fields.get(i).getName();
        return val;
    }

    public static String[] getForInsertFieldsName(Object obj){
        ArrayList<Field> fields = getDAFields(obj);
        String[] val = new String[fields.size()];
        for (int i = 0; i < val.length; i++)
            if(!val.getClass().isAnnotationPresent(SerialPrimaryKey.class)){
                val[i] = fields.get(i).getName();
            }
        return val;
    }

    public static int insert(Connection con, String tabName, Object obj) throws SQLException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Statement st = con.createStatement();
        String[] fieldsName = getForInsertFieldsName(obj);
        String into = Misc.TabToString(",", fieldsName);
        System.out.println("into : "+into);
        StringBuilder insert = new StringBuilder("INSERT INTO " + tabName + "(" + into + ") VALUES (");
        for (int i = 0; i < fieldsName.length; i++) {
            Object attrb = obj.getClass().getMethod("get"+toUpperFirstChar(fieldsName[i])).invoke(obj);
            if(attrb == null) System.out.println("Error : \t" + "get"+toUpperFirstChar(fieldsName[i]));
            insert.append(convertForSql(attrb));
            if(i < fieldsName.length-1) insert.append(",");
        }
        insert.append(")");
        System.out.println(insert);
        int update = st.executeUpdate(insert.toString());
        st.close();
        return update;
    }

    public static int update(Connection con, Object value, String columnLabel,String tabName, String reference) throws SQLException {
        Statement st = con.createStatement();
        String update = "update "+tabName+" set "+columnLabel+" = ";
        update += convertForSql(value);
        update+= " "+reference;
        int val = st.executeUpdate(update);
        st.close();
        return val;
    }

    public static void update(Object[] values, String[] columnLabels,String tabName, String reference, Connection con) throws SQLException{
        for (int i = 0; i < values.length; i++) {
            update(con, values[i], columnLabels[i], tabName, reference);
        }
    }

    private static void setConnection(Connection connection) {
        DAC.connection = connection;
    }
}
