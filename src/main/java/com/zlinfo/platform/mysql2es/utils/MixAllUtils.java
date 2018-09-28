package com.zlinfo.platform.mysql2es.utils;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.util.Properties;

public class MixAllUtils {

    public static void properties2Object(final Properties properties, final Object object) {
        /**
         * 获取配置类的所有方法
         */
        Method[] methods = object.getClass().getMethods();
        for (Method method : methods) {
            String methodName = method.getName();
            if (methodName.startsWith("set")) {
                try {
                    /**
                     * 获取set方法在配置中的值
                     */
                    String tmp = methodName.substring(4);
                    String first = methodName.substring(3, 4);
                    String key = first.toLowerCase() + tmp;
                    String property = properties.getProperty(key);
                    if (property != null) {
                        /**
                         * 获取方法的所有参数，set方法只有一个参数所有是第0个
                         */
                        Class<?>[] paramsTypes = method.getParameterTypes();
                        if (paramsTypes != null && paramsTypes.length > 0) {
                            String type4String = paramsTypes[0].getSimpleName();
                            Object arg = null;
                            /**
                             * 根据参数类型，字符串解释为相应类型的值
                             */
                            if (type4String.equals("int") || type4String.equals("Integer")) {
                                arg = Integer.parseInt(property);
                            } else if (type4String.equals("long") || type4String.equals("Long")) {
                                arg = Long.parseLong(property);
                            } else if (type4String.equals("double") || type4String.equals("Double")) {
                                arg = Double.parseDouble(property);
                            } else if (type4String.equals("boolean") || type4String.equals("Boolean")) {
                                arg = Double.parseDouble(property);
                            } else if (type4String.equals("float") || type4String.equals("Float")) {
                                arg = Float.parseFloat(property);
                            } else if (type4String.equals("String")) {
                                arg = property;
                            } else {
                                continue;
                            }
                            method.invoke(object, arg);
                        }
                    }
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static Properties file2Properties(final String filePath) {
        Properties properties = new Properties();
        File file = new File(filePath);
        InputStream inputStream = null;
        BufferedInputStream buffer = null;

        try {
            inputStream = ClassLoader.getSystemResourceAsStream(filePath);
            buffer = new BufferedInputStream(inputStream);
            properties.load(new InputStreamReader(buffer, "utf-8"));
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                buffer.close();
                inputStream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return properties;
    }
}
