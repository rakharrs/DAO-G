package databaseAccess;

import databaseAccess.exception.DAOException;
import databaseAccess.mapping.PrimaryKey;
import databaseAccess.mapping.View;
import databaseAccess.utils.Misc;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

public class DAO extends DAC{

    /**
     * Update a specific column in the table by using the attributes of the current object as reference
     * @param con Connection to the database
     * @param value new value of the column
     * @param columnLabel name of the column that will be updated
     * @param tabname name of the table
     * @throws SQLException If there is an error with the SQL query or database connection
     * @return number of rows affected by the update query.
     */
    public int update(Connection con, Object value, String columnLabel, String tabname)
            throws NoSuchMethodException, SQLException, InvocationTargetException, IllegalAccessException {
        StringBuilder reference = new StringBuilder("where ");
        String[] attributes = getFieldsName(this);
        for(int i = 0; i < attributes.length; ++i){
            Object attrb = this.getClass().getMethod("get"+ Misc.toUpperFirstChar(attributes[i])).invoke(this);
            reference.append(attributes[i]).append("= ").append(Misc.convertForSql(attrb)).append(" ");
            if(i+1<attributes.length) reference.append("and ");
        }
        return update(con, value, columnLabel, tabname, reference.toString());
    }




    /**
     * Insert this object in the table given by the parameter tabName
     * @param tabName   the name of the table
     * @param con       the connection which will be used
     */
    public int insert(String tabName, Connection con) throws Exception{
        if(this.getClass().isAnnotationPresent(View.class))
            throw new Exception("Can't insert into a view !");
        return insert(con, tabName, this);
    }




    /**
     * build a specific primary key for this object depending on this object primary key annotation
     * @param con the connection that will be used
     * @return the built primary key
     */
    public String buildPrimaryKey(Connection con)
            throws DAOException, SQLException {
        ArrayList<Field> fields = getDAFields(this);
        for(Field field : fields){
            if(field.isAnnotationPresent(PrimaryKey.class) && field.getType() == String.class){
                PrimaryKey pk = field.getAnnotation(PrimaryKey.class);
                return constructPrimaryKey(con, pk);
            }
        }throw new DAOException("No buildable primary key");
    }






    protected String constructPrimaryKey(Connection con, PrimaryKey pk)
            throws DAOException, SQLException {
        String val = "";
        String pkSeq = buildSequenceNb(con, pk);
        String prefix = pk.prefix();
        if(pk.prefix()==null)
            throw new DAOException("No buildable primary key");
        val+=prefix; val+=pkSeq;
        return val;
    }


    protected String buildSequenceNb(Connection con, PrimaryKey pk)
            throws DAOException, SQLException {
        StringBuilder pkSeq = new StringBuilder(new String());
        String seq = getSequence(con, pk);
        for (int i = 0; i < pk.length()-pk.prefix().length()-seq.length(); i++) pkSeq.append("0");
        for (int i = 0; i < seq.length(); i++) pkSeq.append(seq.charAt(i));
        System.out.println(pkSeq);
        return pkSeq.toString();
    }


    protected String getSequence(Connection con, PrimaryKey pk)
            throws DAOException, SQLException {
        if(pk.sequence()==null){
            throw new DAOException("No buildable primary key");
        }
        Statement st = con.createStatement();
        String dbName = con.getMetaData().getDatabaseProductName();
        ResultSet rs = null;
        String val = null;
        switch (dbName) {
            case "PostgreSQL":
                rs = st.executeQuery("SELECT nextval('"+pk.sequence()+"') as nextval");
                break;
            default:
                //rs = st.executeQuery("SELECT "+ getGetSequence() + "as nextval from dual");
                break;
        }
        rs.next();
        val = rs.getString("nextval");
        rs.close(); st.close();
        return val;
    }
}
