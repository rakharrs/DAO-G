package databaseAccess.utils;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

public class Misc {

    /**
     * Converts the first character of a given string to uppercase
     * @param str the input string
     * @return the input string with the first character in uppercase
     */
    public static String toUpperFirstChar(String str){
        return Character.toUpperCase(str.charAt(0))
                + str.substring(1);
    }

    /**
     * return an adapted string for sql syntax
     */
    public static String convertForSql(Object attrb){
        String val = attrb.toString();
        Class<?> AttrClass = attrb.getClass();
        return AttrClass == Date.class ? "TO_DATE('"+attrb+"','YYYY-MM-DD')"
                : (AttrClass == Timestamp.class) ? "TO_TIMESTAMP('"+ attrb +"', 'YYYY-MM-DD HH24:MI:SS.FF')"
                : (AttrClass == String.class) || (AttrClass == Time.class) ? "'"+attrb+"'"
                : attrb.toString();
    }

    public static String TabToString(String regex,String... str){
        StringBuilder val = new StringBuilder();
        for (int i = 0; i < str.length; i++) {
            val.append(str[i]);
            if(i < str.length - 1)
                val.append(regex);
        }
        return val.toString();
    }
}
