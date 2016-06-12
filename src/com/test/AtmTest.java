/*
* Copyright (c) 2016, Dhirendra Chaudhary.
*
* The Universal Permissive License (UPL), Version 1.0
* 
* Subject to the condition set forth below, permission is hereby granted to any person obtaining a copy of this software, associated documentation and/or data (collectively the "Software"), free of charge and under any and all copyright rights in the Software, and any and all patent rights owned or freely licensable by each licensor hereunder covering either (i) the unmodified Software as contributed to or provided by such licensor, or (ii) the Larger Works (as defined below), to deal in both

* (a) the Software, and

* (b) any piece of software and/or hardware listed in the lrgrwrks.txt file if one is included with the Software (each a “Larger Work” to which the Software is contributed by such licensors),
* 
* without restriction, including without limitation the rights to copy, create derivative works of, display, perform, and distribute the Software and make, use, sell, offer for sale, import, export, have made, and have sold the Software and the Larger Work(s), and to sublicense the foregoing rights on either these or other terms.
* 
* This license is subject to the following condition:
* 
* The above copyright notice and either this complete permission notice or at a minimum a reference to the UPL must be included in all copies or substantial portions of the Software.
* 
* THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
* 
* Author: Dhirendra Chaudhary, dhirendra.chaudhary@gmail.com
*/
package com.test;

import java.io.FileInputStream;
import java.io.InputStream;
import java.math.BigInteger;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

/**
 *
 * @author Dhirendra Chaudhary
 */
public class AtmTest {

    Connection con = null;

    public static void main(String args[]) {
        long begin = System.nanoTime();
        Properties props = new Properties();
        InputStream incoming = null;
        try {
            incoming = new FileInputStream("config.properties");
            props.load(incoming);

            String host = props.getProperty("host");
            System.out.println("Host is-->" + host + "\n");

            String dbName = props.getProperty("dbName");
            System.out.println("DbName is-->" + dbName + "\n");

            String userId = props.getProperty("userName");
            System.out.println("UserID is-->" + userId + "\n");

            String pwd = props.getProperty("password");
            System.out.println("Password is-->" + pwd + "\n");

            System.out.println("Properties are : " + host + ", " + dbName);
            new AtmTest().executeProc(host, dbName, userId, pwd);

            System.out.println("Execution time for production Calculation is " + (System.nanoTime() - begin) * 0.000000001 + " seconds");

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                incoming.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }

    public void executeProc(String host, String dbName,String userName, String pwd) {
        try {
            this.con = getMySqlConnection(host, dbName, userName, pwd);

            BigInteger stan = new BigInteger("100000");

            for (int i = 0; i <= 1000; i++) {
                stan = stan.add(new BigInteger("1"));

                String[] inParams = {"01", stan.toString(), "90824001", "001000026201", "", "000000010000", "6072992102599055D23095207860010000000",
                    "999187", "", "C00000000", "423623030009", "622018", "SBI  ZOO GATE DALIBAG  LUCKNOW UPIN", "6072992102599055", "", "", "", "", ""};
                String[] outParams = new String[7];
                CallDBProc("proc-name", inParams, outParams);
                Thread.sleep(1000);
            }
            this.con.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public Connection getMySqlConnection(String host, String dbName, String userName, String pwd) {
        try {
            String url = "jdbc:mysql://" + host + ":3306/" + dbName;
            String driver = "com.mysql.jdbc.Driver";
            Class.forName(driver);

            Connection con = DriverManager.getConnection(url, userName, pwd);
            return con;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    protected void CallDBProc(String procName, String[] inParams, String[] outParams) {
        int n = 0;
        int nInParams = inParams.length;
        int nOutParams = outParams.length;
        String sParams = procName + "(";

        for (n = 0; n < nInParams; n++) {
            sParams = sParams + "'" + inParams[n] + "',";
        }
        // Logger.log(new LogEvent(this, "CallDBProc", sParams));

        CallableStatement statement = null;
        PreparedStatement ps = null;
        try {
            String sProcToCall = "{call " + procName + "(";

            for (n = 0; n < nInParams + nOutParams; n++) {
                sProcToCall = sProcToCall + (n == 0 ? "?" : ",?");
            }
            sProcToCall = sProcToCall + ")}";

            statement = this.con.prepareCall(sProcToCall);

            for (n = 0; n < nInParams; n++) {
                statement.setString(n + 1, inParams[n]);
            }
            for (n = 0; n < nOutParams; n++) {
                statement.registerOutParameter(nInParams + n + 1, 12);
            }
            boolean bDBok = false;
            String sRes = "00";

            for (n = 0; n < 10; n++) {
                statement.executeUpdate();
                sRes = statement.getString(nInParams + 1);

                if (sRes == null) {
                    sRes = "06";
                    //Logger.log(new LogEvent(this, "CallDBProc " + inParams[0], "WARNING: NULL response returned by sp. Setting it to 06."));
                }

                if (!sRes.equalsIgnoreCase("NL")) {
                    bDBok = true;

                    break;
                }

                try {
                    //  this.con.commit();
                    Thread.sleep(100L);
                } catch (InterruptedException iex) {
                }

            }

            if (n > 0) {
                //  Logger.log(new LogEvent(this, "AccountBusy " + inParams[0], bDBok ? "Account locks obtained after " + String.valueOf(n + 1) + " attempts" : "Unable to lock accounts for txn"));
            }

            if (sRes.equalsIgnoreCase("NL")) {
                sRes = "06";
            }
            outParams[0] = sRes;

            if ((nOutParams > 1)) {
                outParams[1] = "C000000000000";
                outParams[2] = "C00000000";

                for (n = 1; n < nOutParams; n++) {
                    outParams[n] = statement.getString(nInParams + n + 1);
                }
            }
//            if (this.setDBMSAppInfo) {
//                ps = this.con.prepareCall("{call dbms_application_info.set_action(null)}");
//                ps.execute();
//            }
            if ((!"00".equalsIgnoreCase(sRes))) {
                this.con.rollback();
                // this.isTimedOut = true;
                outParams[0] = "91";
                outParams[3] = "ERR:Timeout awaiting authorization from host. Pls try again later.";
                outParams[4] = "";
                outParams[5] = "";
                //  Logger.log(new LogEvent(this, "Timeout " + inParams[0], "Rolled back transaction because of its late response."));
            } else {
                // this.con.commit();
            }
           // this.con.close();
            // this.cp.free(this.con);
        } catch (SQLException sqle) {
            try {
                // this.con.rollback();
            } catch (Exception e) {
                System.err.println("Error rolling back transction!");
                e.printStackTrace();
            }
            outParams[0] = "06";
            outParams[1] = "C000000000000";
            outParams[2] = "C00000000";
            outParams[3] = ("ERR:Internal Error ORA-" + (Math.abs(sqle.getErrorCode()) < 10000 ? "0" : "") + String.valueOf(sqle.getErrorCode()));
            try {
               // LogEvent evt = new LogEvent(this, "DBError", sqle.getMessage());
                // Logger.log(evt);
            } catch (Exception e) {
                System.err.println(sParams);
                System.err.println(sqle.toString());
                e.printStackTrace(System.err);

                // jsr 14;
            }
        } finally {
            try {
                if (statement != null) {
                    statement.close();
                }
                if (ps != null) {
                    ps.close();
                }
            } catch (SQLException fse) {
                try {
                    // Logger.log(new LogEvent(this, "DBError2", "Error closing statements: " + fse.getMessage()));
                } catch (Exception e) {
                    System.err.println("Unable to log event: " + fse.getMessage());
                    e.printStackTrace(System.err);
                }
            }
            //this.cp.free(this.con);
            for (int i = 0; i < outParams.length; i++) {
                System.out.println("Param at position " + i + " = " + outParams[i]);
            }
        }
    }
}
