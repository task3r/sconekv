package pt.ulisboa.tecnico.sconekv.common.utils;

import java.util.Properties;

public class PropertiesUtils {
    private PropertiesUtils() {
    }

    public static int getInt(Properties properties, String key) {
        return Integer.parseInt(properties.getProperty(key));
    }

    public static short getShort(Properties properties, String key) {
        return Short.parseShort(properties.getProperty(key));
    }

    public static float getFloat(Properties properties, String key) {
        return Float.parseFloat(properties.getProperty(key));
    }

    public static long getLong(Properties properties, String key) {
        return Long.parseLong(properties.getProperty(key));
    }

    public static String getString(Properties properties, String key) {
        return properties.getProperty(key);
    }
}
