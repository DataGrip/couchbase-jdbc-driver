package com.dbschema;

import java.sql.SQLException;

/**
 * @author Liudmila Kornilova
 **/
public class SSLParamsException extends SQLException {
  public SSLParamsException(String message, Throwable cause) {
    super(message, cause);
  }
}
