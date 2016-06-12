
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

import java.util.Date;
import org.jpos.core.VolatileSequencer;
import org.jpos.iso.ISODate;
import org.jpos.iso.ISOException;
import org.jpos.iso.ISOMsg;
import org.jpos.iso.MUX;
import org.jpos.q2.QBeanSupport;
import org.jpos.util.NameRegistrar;

public class HostSignOn extends QBeanSupport implements Runnable {

    boolean signedOn = false;
    VolatileSequencer seq;

    @Override
    protected void initService()throws Exception {
        this.seq = new VolatileSequencer();
        new Thread(this).start();
        super.initService();
    }

    @Override
    public void run() {
        try {
            Thread.sleep(5000L);
        } catch (InterruptedException e) {
        }

        long sleepTime = this.cfg.getLong("pingIntervalSec", 10L) * 1000L;
        while (true) {
            if (running()) {
                try {
                    MUX mux;
                    try {
                        mux = (MUX) NameRegistrar.get("mux." + this.cfg.get("mux"));
                    } catch (NameRegistrar.NotFoundException e) {
                        System.err.println("HostSignOn: could not get mux " + this.cfg.get("mux"));
                        mux = null;
                    }
                    if (mux != null) {
                        if (!this.signedOn) {
                            System.err.println("HostSignOn: not signed on. Will send a sign-on message to mux now...");
                            ISOMsg s = new ISOMsg("0800");
                            s.set(7, ISODate.getDateTime(new Date()));
                            s.set(11, "000001");
                            s.set(70, "001");
                            this.seq.set("awmhost", 2);
                            ISOMsg r = mux.request(s, 5000L);
                            System.err.println("HostSignOn: response to sign-on message is " + r);
                            if (r != null) {
                                this.signedOn = true;
                            }
                        }
                    }
                    Thread.sleep(sleepTime);
                } catch (ISOException e) {
                    
                } catch (InterruptedException x) {
                    
                }
            }
        }
    }
}