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
import java.util.Date;
import java.util.Vector;
import org.jpos.core.Configurable;
import org.jpos.core.Configuration;
import org.jpos.core.ConfigurationException;
import org.jpos.core.ReConfigurable;
import org.jpos.iso.ISOChannel;
import org.jpos.iso.ISODate;
import org.jpos.iso.ISOException;
import org.jpos.iso.ISOMsg;
import org.jpos.iso.ISORequestListener;
import org.jpos.iso.ISOSource;
import org.jpos.util.LogEvent;
import org.jpos.util.LogSource;
import org.jpos.util.Logger;

public class RequestDispatcher implements ISORequestListener, LogSource, Configurable, ReConfigurable {

    Logger logger;
    String logRealm;
    String poolName = "srvtdb";
    String broadcastMsg = "";
    String unhandledRespCode = "00";
    String txnProcName;
    String reversalProcName;
    String offusProcName;
    boolean setDBMSAppInfo = false;
    boolean enableLocalAuthID = false;
    Configuration cfg;
    Vector singedOnHosts;
    long txnTimeout = 0L;

    public RequestDispatcher() {
        this.singedOnHosts = new Vector();
    }

    public static void main(String[] args) {
        new RequestDispatcher();
    }

    @Override
    public boolean process(ISOSource source, ISOMsg m) {
        String mti = "";
        String src = "";
        try {
            if (!m.isResponse()) {
                mti = m.getMTI();
                if ("0800".equalsIgnoreCase(mti)) {
                    ISOMsg r = (ISOMsg) m.clone();
                    r.setResponseMTI();
                    source.send(r);
                    if ((source instanceof ISOChannel)) {
                        src = ((ISOChannel) source).toString();
                    }
                    if (("301".equals(m.getString(70))) && (src.length() > 0) && (!this.singedOnHosts.contains(src))) {
                        ISOMsg s = new ISOMsg("0800");
                        s.set(7, ISODate.getDateTime(new Date()));
                        s.set(11, "000001");
                        s.set(70, "001");
                        source.send(s);
                        this.singedOnHosts.add(src);
                        if (this.logger != null) {
                            Logger.log(new LogEvent(this, "SignOn", "Sent sign-on to \"" + src + "\""));
                        } else {
                            System.err.println("Sent sign-on to \"" + src + "\"");
                        }
                    }
                } else if (("0200".equalsIgnoreCase(mti)) || ("0220".equalsIgnoreCase(mti)) || ("0100".equalsIgnoreCase(mti)) || ("0420".equalsIgnoreCase(mti))) {
                    MessageHandler handler = new MessageHandler(this.poolName, source, m, this.broadcastMsg, this.setDBMSAppInfo, this.enableLocalAuthID, this.txnProcName, this.reversalProcName, this.offusProcName);
                    handler.setLogger(this.logger, this.logRealm + "MH");
                    handler.setTxnTimeout(this.txnTimeout);
                    new Thread(handler).start();
                } else {
                    ISOMsg r = (ISOMsg) m.clone();
                    r.setResponseMTI();
                    r.set(39, this.unhandledRespCode);
                    if (this.logger != null) {
                        Logger.log(new LogEvent(this, "Unhandled", m.toString() + " MTI=" + mti));
                    } else {
                        System.err.println("[No handler]" + m.toString() + " MTI=" + mti);
                    }
                    source.send(r);
                }
            }
            return true;
        } catch (ISOException e) {
            if (this.logger != null) {
                Logger.log(new LogEvent(this, "ProcessMsg", e));
            } else {
                e.printStackTrace();
            }
            return true;
        } catch (IOException e) {
            if (this.logger != null) {
                Logger.log(new LogEvent(this, "SendResponse", e));
            } else {
                e.printStackTrace();
            }
        }
        return false;
    }

    @Override
    public void setLogger(Logger logger, String realm) {
        this.logger = logger;
        this.logRealm = realm;
    }

    @Override
    public String getRealm() {
        return this.logRealm;
    }

    @Override
    public Logger getLogger() {
        return this.logger;
    }

    @Override
    public void setConfiguration(Configuration cfg) throws ConfigurationException {
        this.cfg = cfg;
        this.poolName = cfg.get("poolName", "srvtdb");
        this.broadcastMsg = cfg.get("broadcast", "");
        this.setDBMSAppInfo = cfg.getBoolean("setDBMSAppInfo", false);
        this.unhandledRespCode = cfg.get("unhandledRespCode", "00");
        this.enableLocalAuthID = cfg.getBoolean("enableLocalAuthID", false);
        this.txnTimeout = cfg.getLong("txnTimeoutMS", 0L);
        this.txnProcName = cfg.get("txnProcedure", "admin.bao_process_isomsg");
        this.reversalProcName = cfg.get("reversalProcedure", "admin.bao_process_reversal");
        this.offusProcName = cfg.get("offusProcedure", "admin.bao_process_offus_tran");
    }
}
