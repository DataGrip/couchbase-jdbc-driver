package com.dbschema;

import java.sql.DriverPropertyInfo;
import java.util.ArrayList;
import java.util.Locale;

public class DriverPropertyInfoHelper {
  public static final String VERIFY_SERVER_CERTIFICATE = "verifyServerCertificate";
  public static final String VERIFY_SERVER_CERTIFICATE_DEFAULT = "true";


  public static DriverPropertyInfo[] getPropertyInfo() {
    ArrayList<DriverPropertyInfo> propInfos = new ArrayList<>();

    addPropInfo(propInfos, VERIFY_SERVER_CERTIFICATE, VERIFY_SERVER_CERTIFICATE_DEFAULT,
        "Configure a connection that uses SSL but does not verify the identity of the server.", null);

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

  public static boolean isFalse(String value) {
    return value == null || value.equals("0") || value.toLowerCase(Locale.ENGLISH).equals("false");
  }
}
