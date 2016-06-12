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
package digital.megh.atm;

import java.io.IOException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.jpos.core.CardHolder;
import org.jpos.core.InvalidCardException;
import org.jpos.iso.ISOException;
import org.jpos.iso.ISOMsg;
import org.jpos.iso.ISOSource;
import org.jpos.iso.LeftPadder;
import org.jpos.iso.RightPadder;
import org.jpos.tpl.ConnectionPool;
import org.jpos.util.LogEvent;
import org.jpos.util.LogSource;
import org.jpos.util.Logger;
import org.jpos.util.NameRegistrar;

public class MessageHandler implements Runnable, LogSource {

    Logger logger;
    String logRealm;
    ConnectionPool cp;
    Connection con;
    String poolName;
    ISOSource source;
    ISOMsg m;
    boolean setDBMSAppInfo;
    boolean enableLocalAuthID;
    String broadcastMsg;
    SimpleDateFormat sf;
    String reversedSTAN;
    boolean isOffusTran;
    boolean isOffusReversal;
    String procCode;
    String txnProc;
    String reversalProc;
    String offusProc;
    long timeStartRun;
    long timeStartGetCon = 0L;
    long timeEndGetCon = 0L;
    long timeStartCall = 0L;
    long timeEndCall = 0L;
    long timeStartSend = 0L;
    long timeEndRun = 0L;
    long txnTimeout = 0L;
    boolean isTimedOut;
    private final int BIT36MAXLENGTH = 104;
    private final int MAXINFOOUTSIZE = 170;
    private final int MAXPAYLOADSIZE = 480;
    private final int MINB121LENGTH = 40;
    public static final int SIF_PROC_CODE = 3;
    public static final int SIF_ACCOUNT_NO = 2;
    public static final int SIF_AMOUNT = 4;
    public static final int SIF_STAN = 11;
    public static final int SIF_POS_ENTRY_MODE = 22;
    public static final int SIF_ACQUIRER_ID = 32;
    public static final int SIF_TRACK2 = 35;
    public static final int SIF_INFO_IN = 36;
    public static final int SIF_RTRVL_REF = 37;
    public static final int SIF_AUTH_ID = 38;
    public static final int SIF_TERMINAL_ID = 41;
    public static final int SIF_TERMINAL_LOCATION = 43;
    public static final int SIF_EXT_PAN = 48;
    public static final int SIF_OLD_DATA = 90;
    public static final int SIF_AUTH_AMOUNT = 95;
    public static final int SIF_FROM_ACCOUNT = 102;
    public static final int SIF_TO_ACCOUNT = 103;
    public static final int SOF_AMOUNT_SETTLEMENT = 5;
    public static final int SOF_AMOUNT_BILLING = 6;
    public static final int SOF_CONV_RATE_SETTLEMENT = 9;
    public static final int SOF_CONV_RATE_BILLING = 10;
    public static final int SOF_FEE = 28;
    public static final int SOF_INFO_OUT = 36;
    public static final int SOF_RESPONSE_CODE = 39;
    public static final int SOF_CURRENCY_SETTLEMENT = 50;
    public static final int SOF_CURRENCY_BILLING = 51;
    public static final int SOF_BALANCES = 54;
    public static final int SOF_DATA120 = 120;
    public static final int SOF_DATA121 = 121;
    public static final String RESP_CODE_HOST_UNAVAILABLE = "91";
    public static final String RESP_CODE_NO_LOCK = "NL";
    public static final String RESP_CODE_GENERIC_ERROR = "06";
    public static final String PROC_CODE_GLTRAN = "19";
    public static final String MSG_HOST_TIMEOUT = "ERR:Timeout awaiting authorization from host. Pls try again later.";

    public MessageHandler(String pool, ISOSource src, ISOMsg msg, String broadcast, boolean setDBMSAppInfo, boolean enableLocalAuthID, String txnProc, String reversalProc, String offusProc) {
        this.poolName = pool;
        this.broadcastMsg = broadcast;
        this.setDBMSAppInfo = setDBMSAppInfo;
        this.enableLocalAuthID = enableLocalAuthID;
        this.m = msg;
        this.source = src;
        this.timeStartRun = System.currentTimeMillis();
        this.sf = new SimpleDateFormat("yyyyMMddHHmmss.SSS");
        this.reversedSTAN = "";
        this.procCode = (msg.hasField(3) ? msg.getString(3).substring(0, 2) : "00");

        this.isOffusTran = false;
        this.isOffusReversal = false;
        this.txnProc = txnProc;
        this.reversalProc = reversalProc;
        this.offusProc = offusProc;

        if ("19".equalsIgnoreCase(this.procCode)) {
            try {
                this.isOffusTran = "0220".equalsIgnoreCase(msg.getMTI());
                this.isOffusReversal = "0420".equalsIgnoreCase(msg.getMTI());
            } catch (ISOException e) {
            }
        }
    }

    public void setReversalStan(String stan) {
        this.reversedSTAN = stan;
    }

    public void run() {
        String[] inP = new String[this.m.hasField(90) ? 15 : (this.isOffusTran) || (this.isOffusReversal) ? 9 : 14];

        String[] outP = new String[(this.isOffusTran) || (this.isOffusReversal) ? 1 : 7];
        String preFix = "";
        boolean b121Set = false;
        boolean respOk = false;
        String procName = (this.isOffusTran) || (this.isOffusReversal) ? this.offusProc : this.txnProc;
        String RespCode = "";
        boolean isLocalAuth = false;
        String authId = "";
        try {
            this.timeStartGetCon = System.currentTimeMillis();
            this.cp = ConnectionPool.getConnectionPool(this.poolName);

            this.con = this.cp.getConnection();
            this.timeEndGetCon = System.currentTimeMillis();

            if ((this.isOffusReversal) || (this.isOffusTran)) {
                inP[0] = this.procCode;
                inP[3] = this.m.getString(41);
                inP[4] = this.m.getString(11);
                inP[5] = this.m.getString(38);
                inP[6] = "";
                try {
                    if (this.m.hasField(35)) {
                        inP[6] = new CardHolder(this.m.getString(35)).getPAN();
                    }
                } catch (InvalidCardException e) {
                }
                inP[7] = this.m.getString(2);

                if (this.isOffusTran) {
                    inP[1] = "0";
                    inP[2] = this.m.getString(4);
                    inP[8] = "";
                } else if (this.isOffusReversal) {
                    inP[1] = "1";
                    if (this.m.hasField(90)) {
                        try {
                            if (this.m.hasField(95)) {
                                inP[2] = this.m.getString(95).substring(0, 12);
                            } else {
                                inP[2] = "0";
                            }
                        } catch (IndexOutOfBoundsException e) {
                            inP[2] = "0";
                        }
                        try {
                            inP[8] = this.m.getString(90).substring(4, 10);
                        } catch (IndexOutOfBoundsException e) {
                            inP[8] = this.m.getString(90);
                        }
                    }
                }
            } else {
                inP[0] = this.procCode;
                inP[1] = this.m.getString(11);
                inP[2] = this.m.getString(41);
                inP[3] = (this.m.hasField(102) ? this.m.getString(102) : this.m.getString(2));

                inP[4] = this.m.getString(103);
                inP[6] = this.m.getString(35);
                if ((this.enableLocalAuthID) && ((inP[0].equalsIgnoreCase("18")) || (inP[0].equalsIgnoreCase("28")) || (inP[0].equalsIgnoreCase("46")))) {
                    isLocalAuth = true;
                    authId = LeftPadder.ZERO_PADDER.pad(String.valueOf(getNextAuthID()), 6);
                    inP[7] = authId;
                } else {
                    inP[7] = this.m.getString(38);
                }
                inP[8] = this.m.getString(36);
                inP[9] = this.m.getString(28);
                inP[10] = this.m.getString(37);
                inP[11] = this.m.getString(32);
                if (this.m.hasField(43)) {
                    inP[12] = this.m.getString(43).trim();
                } else {
                    inP[12] = null;
                }
                inP[13] = this.m.getString(48);
                if (this.m.hasField(90)) {
                    procName = this.reversalProc;
                    try {
                        if (this.m.hasField(95)) {
                            inP[5] = this.m.getString(95).substring(0, 12);
                        } else {
                            inP[5] = "0";
                        }
                    } catch (IndexOutOfBoundsException e) {
                        inP[5] = "0";
                    }
                    try {
                        inP[14] = this.m.getString(90).substring(4, 10);
                    } catch (IndexOutOfBoundsException e) {
                        inP[14] = this.m.getString(90);
                    }
                } else {
                    inP[5] = this.m.getString(4);
                }
            }
            this.isTimedOut = false;
            this.timeStartCall = System.currentTimeMillis();
            CallDBProc(procName, inP, outP);
            this.timeEndCall = System.currentTimeMillis();

            ISOMsg r = (ISOMsg) this.m.clone();
            r.setResponseMTI();
            if ((this.isOffusReversal) || (this.isOffusTran)) {
                r.set(39, "00");
            } else {
                if (isLocalAuth) {
                    r.set(38, authId);
                }
                r.set(5, "000000000000");
                r.set(6, "000000000000");
                r.set(9, "61000000");
                r.set(10, "61000000");
                r.set(50, "356");
                r.set(51, "356");
                if ((outP[1] == null) || (!outP[1].matches("[CD]([0-9]{12})$"))) {
                    System.err.println(this.m.getString(38) + ": bad net balance: " + outP[1] + ", will send C000000000000");

                    outP[1] = "C000000000000";
                }
                if ((outP[6] == null) || (!outP[6].matches("[CD]([0-9]{12})$"))) {
                    if ((outP[6] != null) && (outP[6].length() > 0)) {
                        System.err.println(this.m.getString(38) + ": bad ledger balance: " + outP[6] + ", will send " + outP[1]);
                    }
                    outP[6] = outP[1];
                }
                r.set(54, (this.m.hasField(3) ? this.m.getString(3).substring(2, 4) : "10") + "01356" + outP[1] + (this.m.hasField(3) ? this.m.getString(3).substring(2, 4) : "10") + "02356" + outP[6]);

                if ((outP[2] == null) || (!outP[2].matches("C([0-9]{8})$"))) {
                    System.err.println(this.m.getString(38) + ": bad fee: " + outP[2] + ", will send C00000000");

                    outP[2] = "C00000000";
                }
                r.set(28, outP[2]);
                r.set(39, outP[0]);

                if ("35".equalsIgnoreCase(inP[0])) {
                    if ("98".equalsIgnoreCase(outP[0])) {
                        preFix = "1";
                        r.set(39, "00");
                    } else {
                        preFix = "0";
                    }
                } else {
                    preFix = "";
                }

                r.unset(new int[]{36, 120, 121});

                if ((outP[5] != null)
                        && (outP[5].length() > 0)) {
                    r.set(121, preFix + outP[5]);
                    b121Set = true;
                }

                if (!b121Set) {
                    if ((outP[3] != null)
                            && (outP[3].length() > 0)) {
                        r.set(36, outP[3]);
                    }
                    if ((outP[4] != null)
                            && (outP[4].length() > 0)) {
                        r.set(120, outP[4]);
                    }
                }
                setMsgFields(r);
            }
            this.timeStartSend = System.currentTimeMillis();
            this.source.send(r);
            respOk = true;
            RespCode = r.getString(39);
        } catch (NameRegistrar.NotFoundException e) {
            if (this.logger != null) {
                Logger.log(new LogEvent(this, "GetConnectionPool", e));
            } else {
                e.printStackTrace();
            }
        } catch (SQLException e) {
            if (this.logger != null) {
                Logger.log(new LogEvent(this, "GetConnection", e));
            } else {
                e.printStackTrace();
            }
        } catch (ISOException e) {
            if (this.logger != null) {
                Logger.log(new LogEvent(this, "PrepareResponse", e));
            } else {
                e.printStackTrace();
            }
        } catch (IOException e) {
            if (this.logger != null) {
                Logger.log(new LogEvent(this, "SendResponse", e));
            } else {
                e.printStackTrace();
            }
        } finally {
            if (!respOk) {
                try {
                    ISOMsg r = (ISOMsg) this.m.clone();
                    r.setResponseMTI();
                    r.set(39, "06");
                    r.unset(new int[]{36, 120, 121});

                    r.set(5, "000000000000");
                    r.set(6, "000000000000");
                    r.set(9, "61000000");
                    r.set(10, "61000000");
                    r.set(50, "356");
                    r.set(51, "356");
                    r.set(54, (this.m.hasField(3) ? this.m.getString(3).substring(2, 4) : "10") + "02356C000000000000");

                    r.set(28, "C00000000");
                    this.timeStartSend = System.currentTimeMillis();
                    this.source.send(r);
                    RespCode = r.getString(39);
                } catch (Exception e) {
                }
            }

            this.timeEndRun = System.currentTimeMillis();
            System.out.println(this.sf.format(new Date()) + "\t" + inP[1] + "\t" + inP[0] + "\t" + (this.timeEndRun - this.timeStartRun) + "\t" + (this.timeEndGetCon - this.timeStartGetCon) + "\t" + (this.timeEndCall - this.timeStartCall) + "\t" + (this.timeEndRun - this.timeStartSend) + "\t" + RespCode + (this.isTimedOut ? "\tTIMEOUT" : ""));
        }
    }

    public void setLogger(Logger logger, String realm) {
        this.logger = logger;
        this.logRealm = realm;
    }

    public String getRealm() {
        return this.logRealm;
    }

    public Logger getLogger() {
        return this.logger;
    }

    protected void CallDBProc(String procName, String[] inParams, String[] outParams) {
        int n = 0;
        int nInParams = inParams.length;
        int nOutParams = outParams.length;
        String sParams = procName + "(";

        for (n = 0; n < nInParams; n++) {
            sParams = sParams + "'" + inParams[n] + "',";
        }
        Logger.log(new LogEvent(this, "CallDBProc", sParams));

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
                statement.execute();
                sRes = statement.getString(nInParams + 1);

                if (sRes == null) {
                    sRes = "06";
                    Logger.log(new LogEvent(this, "CallDBProc " + inParams[0], "WARNING: NULL response returned by sp. Setting it to 06."));
                }

                if (!sRes.equalsIgnoreCase("NL")) {
                    bDBok = true;

                    break;
                }

                try {
                    //this.con.commit();
                    Thread.sleep(100L);
                } catch (InterruptedException iex) {
                }

            }

            if (n > 0) {
                Logger.log(new LogEvent(this, "AccountBusy " + inParams[0], bDBok ? "Account locks obtained after " + String.valueOf(n + 1) + " attempts" : "Unable to lock accounts for txn"));
            }

            if (sRes.equalsIgnoreCase("NL")) {
                sRes = "06";
            }
            outParams[0] = sRes;

            if ((nOutParams > 1) && (!this.isOffusReversal) && (!this.isOffusTran)) {
                outParams[1] = "C000000000000";
                outParams[2] = "C00000000";

                for (n = 1; n < nOutParams; n++) {
                    outParams[n] = statement.getString(nInParams + n + 1);
                }
            }
            if (this.setDBMSAppInfo) {
                ps = this.con.prepareCall("{call dbms_application_info.set_action(null)}");
                ps.execute();
            }
            if ((!"00".equalsIgnoreCase(sRes)) && (this.txnTimeout > 0L) && (System.currentTimeMillis() - this.timeStartCall > this.txnTimeout) && (!this.isOffusReversal) && (!this.isOffusTran)) {
                this.con.rollback();
                this.isTimedOut = true;
                outParams[0] = "91";
                outParams[3] = "ERR:Timeout awaiting authorization from host. Pls try again later.";
                outParams[4] = "";
                outParams[5] = "";
                Logger.log(new LogEvent(this, "Timeout " + inParams[0], "Rolled back transaction because of its late response."));
            } else {
                //this.con.commit();
            }
            this.con.close();
            this.cp.free(this.con);
        } catch (SQLException sqle) {
            try {
                this.con.rollback();
            } catch (Exception e) {
                System.err.println("Error rolling back transction!");
                e.printStackTrace();
            }
            outParams[0] = "06";
            outParams[1] = "C000000000000";
            outParams[2] = "C00000000";
            outParams[3] = ("ERR:Internal Error ORA-" + (Math.abs(sqle.getErrorCode()) < 10000 ? "0" : "") + String.valueOf(sqle.getErrorCode()));
            try {
                LogEvent evt = new LogEvent(this, "DBError", sqle.getMessage());
                Logger.log(evt);
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
                    Logger.log(new LogEvent(this, "DBError2", "Error closing statements: " + fse.getMessage()));
                } catch (Exception e) {
                    System.err.println("Unable to log event: " + fse.getMessage());
                    e.printStackTrace(System.err);
                }
            }
            this.cp.free(this.con);
        }
    }

    protected void setMsgFields(ISOMsg m)
            throws ISOException {
        String bit36 = "";
        String bit120 = "";
        String bit121 = "";

        if (m.hasField(36)) {
            bit36 = m.getString(36);
        }
        if (m.hasField(120)) {
            bit120 = m.getString(120);
        }
        if (m.hasField(121)) {
            bit121 = m.getString(121);
        }
        int infoOutSize = bit36.length() + bit120.length();

        if ((bit121 == "") && ((m.getMTI().startsWith("02")) || (m.getMTI().startsWith("01"))) && (this.broadcastMsg.trim().length() > 0)) {
            m.unset(36);
            m.unset(120);

            if (infoOutSize > 0) {
                bit121 = "0^1:" + bit36 + "^2:" + bit120 + "^M:" + this.broadcastMsg;
            } else {
                bit121 = (m.hasField(121) ? m.getString(121) : "0") + "^M:" + this.broadcastMsg;
            }
        } else if ((infoOutSize > 170) || ((bit36.length() > 104) && (infoOutSize > 167))) {
            m.unset(36);
            m.unset(120);

            if (m.hasField(121)) {
                bit121 = bit121 + "^1:" + bit36 + "^2:" + bit120;
            } else {
                bit121 = "0^1:" + bit36 + "^2:" + bit120;
            }
            infoOutSize = 0;
        } else if (bit36.length() > 104) {
            m.set(36, bit36.substring(0, 102) + "^^");
            m.set(120, bit36.substring(102) + "^" + bit120);
        }

        if (bit121.length() > 0) {
            int bit121len = bit121.length();

            if (bit121len > 480) {
                System.err.println("WARNING: " + m.getString(38) + ": bit121 too long (" + String.valueOf(bit121len) + ", will be truncated to " + String.valueOf(480));

                bit121 = bit121.substring(0, 480);
                bit121len = bit121.length();
            }
            if (bit121len < 40) {
                bit121 = RightPadder.SPACE_PADDER.pad(bit121, 40);
            }

            m.set(121, bit121);

            if (m.hasField(36)) {
                m.unset(36);
            }
            if (m.hasField(120)) {
                m.unset(120);
            }
        }
    }

    private int getNextAuthID() {
        int authId = -1;
        try {
            CallableStatement statement = this.con.prepareCall("{? = call admin.bau_get_next_auth_id()}");
            statement.registerOutParameter(1, 4);
            statement.execute();
            authId = statement.getInt(1);
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return authId;
    }

    public long getTxnTimeout() {
        return this.txnTimeout;
    }

    public void setTxnTimeout(long txnTimeout) {
        this.txnTimeout = txnTimeout;
    }
}