package com.intellij;

import java.sql.DriverPropertyInfo;
import java.util.ArrayList;
import java.util.Locale;

public class DriverPropertyInfoHelper {
  public static final String ENABLE_SSL = "sslenabled";
  public static final String ENABLE_SSL_DEFAULT = "false";
  public static final String USER = "user";
  public static final String PASSWORD = "password";
  public static final String META_SAMPLING_SIZE = "meta.sampling.size";
  public static final int META_SAMPLING_SIZE_DEFAULT = 1000;
  private static final String[] choices = new String[]{"true", "false"};


  public static DriverPropertyInfo[] getPropertyInfo() {
    ArrayList<DriverPropertyInfo> propInfos = new ArrayList<>();

    addPropInfo(propInfos, ENABLE_SSL, ENABLE_SSL_DEFAULT, "Enable ssl.", choices);
    addPropInfo(propInfos, USER, "", "Username.", null);
    addPropInfo(propInfos, PASSWORD, "", "Password.", null);
    addPropInfo(propInfos, META_SAMPLING_SIZE, Integer.toString(META_SAMPLING_SIZE_DEFAULT),
            "Number of documents that will be fetched per collection in order " +
            "to return meta information from DatabaseMetaData.getColumns method.", null);

    return propInfos.toArray(new DriverPropertyInfo[0]);
  }

  private static void addPropInfo(final ArrayList<DriverPropertyInfo> propInfos, final String propName,
                                  final String defaultVal, final String description, final String[] choices) {
    DriverPropertyInfo newProp = new DriverPropertyInfo(propName, defaultVal);
    newProp.description = description;
    if (choices != null) {
      newProp.choices = choices;
    }
    propInfos.add(newProp);
  }

  public static boolean isTrue(String value) {
    return value != null && (value.equals("1") || value.toLowerCase(Locale.ENGLISH).equals("true"));
  }
}
