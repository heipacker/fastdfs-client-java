/**
 * Copyright (C) 2008 Happy Fish / YuQing
 * <p>
 * FastDFS Java Client may be copied only under the terms of the GNU Lesser
 * General Public License (LGPL).
 * Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
 **/

package org.csource.common;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * ini file reader / parser
 *
 * @author Happy Fish / YuQing
 * @version Version 1.0
 */
public class IniFileReader {

    private Map<String, Object> configMap = new HashMap<String, Object>();
    ;
    private String confFilename;

    /**
     * @param confFilename config filename
     */
    public IniFileReader(String confFilename) throws IOException {
        this.confFilename = confFilename;
        loadFromFile(confFilename);
    }

    /**
     * get the config filename
     *
     * @return config filename
     */
    public String getConfFilename() {
        return confFilename;
    }

    /**
     * get string value from config file
     *
     * @param name item name in config file
     * @return string value
     */
    public String getStrValue(String name) {
        Object value = this.configMap.get(name);
        if (value == null) {
            return null;
        }
        if (value instanceof String) {
            return (String) value;
        }
        return ((List<String>) value).get(0);
    }

    /**
     * get int value from config file
     *
     * @param name         item name in config file
     * @param defaultValue the default value
     * @return int value
     */
    public int getIntValue(String name, int defaultValue) {
        String value = getStrValue(name);
        if (value == null) {
            return defaultValue;
        }
        return Integer.parseInt(value);
    }

    /**
     * get boolean value from config file
     *
     * @param name          item name in config file
     * @param default_value the default value
     * @return boolean value
     */
    public boolean getBoolValue(String name, boolean default_value) {
        String value = this.getStrValue(name);
        if (value == null) {
            return default_value;
        }
        value = value.toLowerCase();
        return value.equals("yes") || value.equals("on") || value.equals("true") || value.equals("1");
    }

    /**
     * get all values from config file
     *
     * @param name item name in config file
     * @return string values (array)
     */
    public String[] getValues(String name) {
        Object value = this.configMap.get(name);
        if (value == null) {
            return null;
        }
        String[] values;
        if (value instanceof String) {
            values = new String[1];
            values[0] = (String) value;
            return values;
        }

        Object[] objects = ((List<String>) value).toArray();
        values = new String[objects.length];
        System.arraycopy(objects, 0, values, 0, objects.length);
        return values;
    }

    private void loadFromFile(String confFilename) throws IOException {
        BufferedReader buffReader = new BufferedReader(new FileReader(confFilename));
        try {
            String line;
            while ((line = buffReader.readLine()) != null) {
                if (line.length() == 0 || line.trim().startsWith("#")) {
                    continue;
                }
                String[] parts = line.split("=", 2);
                if (parts.length != 2) {
                    continue;
                }
                String name = parts[0].trim();
                String value = parts[1].trim();
                Object obj = configMap.get(name);
                if (obj == null) {
                    configMap.put(name, value);
                } else if (obj instanceof String) {
                    List<String> valueList = new ArrayList<String>();
                    valueList.add((String) obj);
                    valueList.add(value);
                    configMap.put(name, valueList);
                } else {
                    List<String> valueList = (ArrayList<String>) obj;
                    valueList.add(value);
                }
            }
        } finally {
            buffReader.close();
        }
    }
}
