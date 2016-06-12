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
package com.test.atm.client;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.Socket;
import org.jpos.iso.ISOException;
import org.jpos.iso.ISOMsg;
import org.jpos.iso.ISOUtil;
import org.jpos.iso.packager.GenericPackager;

/**
 *
 * @author Dhirendra Chaudhary
 */
public class ATMClient {

    public static void main(String args[]) {
        try {
            GenericPackager packager = new GenericPackager(iso87ascii.xml");
            String data = "02520200723A80112CA1C01014001430201964163010000000000000000115160126574968160126011501150115C00000000041111376073122014108147D1301126183094460000040151657496857496890993001TEST ATM PUNE          PUNE         MHIN01660731220141081473560000162014201420142014";
            // Create ISO Message
            ISOMsg isoMsg = new ISOMsg();
            isoMsg.setPackager(packager);
//            //isoMsg.setMTI("0200");
//            isoMsg.set(0,"0200");
//            isoMsg.set(2, "011000087601");
//            isoMsg.set(3, "011000");
//            isoMsg.set(4, "000000010000");
//            isoMsg.set(7, ISODate.getDateTime(new Date()));
//            isoMsg.set(11, "999187");
//            isoMsg.set(12, "233105");
//            isoMsg.set(13, "0824");
//            isoMsg.set(15, "0824");
//            isoMsg.set(17, "0824");
//            isoMsg.set(28, "C00000000");
//            isoMsg.set(32, "622018");
//            isoMsg.set(35, "6072992102599055D23095207860010000000");
//            isoMsg.set(37, "423623030009");
//            isoMsg.set(38, "999187");
//            isoMsg.set(41, "12221050");
//            isoMsg.set(43, "SBI  ZOO GATE DALIBAG  LUCKNOW      UPIN");
//            isoMsg.set(48, "6072992102599055");
//            isoMsg.set(49, "356");
//            isoMsg.set(50, "000");
//            isoMsg.set(59, "000");
//            isoMsg.set(60, "2014201420142014");
//            isoMsg.set(61, "2014201420142014");


//      <field id="2" value="011000087601"/>
//      <field id="3" value="011000"/>
//      <field id="4" value="000000010000"/>
//      <field id="7" value="0824233103"/>
//      <field id="11" value="999187"/>
//      <field id="12" value="233105"/>
//      <field id="13" value="0824"/>
//      <field id="15" value="0824"/>
//      <field id="17" value="0824"/>
//      <field id="28" value="C00000000"/>
//      <field id="32" value="622018"/>
//      <field id="35" value="6072992102599055D23095207860010000000"/>
//      <field id="37" value="423623030009"/>
//      <field id="38" value="999187"/>
//      <field id="41" value="12221050"/>
//      <field id="43" value="SBI  ZOO GATE DALIBAG  LUCKNOW      UPIN"/>
//      <field id="48" value="6072992102599055"/>
//      <field id="49" value="356"/>
//      <field id="50" value="000"/>
//      <field id="60" value="2014201420142014"/>
//                // print the DE list
            //logISOMsg(isoMsg);

            // Get and print the output result
            int i = isoMsg.unpack(data.getBytes());
            System.out.println("RESULT : " + new String(data));
            logISOMsg(isoMsg);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    private Socket socketTCP;
    public static int PORT = 5042;
    private BufferedOutputStream outStream = null;
    private BufferedReader receivedData;

    public void sendReceive(byte[] byteArr) {
        try {
            GenericPackager packager = new GenericPackager("iso87ascii-binary-bitmap.xml");
            String remoteHost = "192.168.2.23";

            socketTCP = new Socket(remoteHost, PORT);
            if (socketTCP.isConnected()) {
                outStream = new BufferedOutputStream(socketTCP.getOutputStream());
                outStream.write(byteArr);
                outStream.flush();
                System.out.println("-----Request sent to server---");
                System.out.println(ISOUtil.hexString(byteArr));
            }

            if (socketTCP.isConnected()) {
                receivedData = new BufferedReader(
                        new InputStreamReader(socketTCP.getInputStream()));
                System.out.println("-- RESPONSE recieved---");
               // byte[] barr = receivedData.readLine().getBytes();
              //  System.out.println(ISOUtil.hexString(barr));
               // ISOMsg isoMsg = new ISOMsg();
               // isoMsg.setPackager(packager);
               // isoMsg.unpack(barr);
               // logISOMsg(isoMsg);
            }

            receivedData.close();
            outStream.close();
            socketTCP.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void logISOMsg(ISOMsg msg) {
        System.out.println("----ISO MESSAGE-----");
        try {
            System.out.println("  MTI : " + msg.getMTI());
            for (int i = 1; i <= msg.getMaxField(); i++ ) {
				if (msg.hasField(i)) {
                    System.out.println("    Field-" + i + " : " + msg.getString(i));
                }
            }
        } catch (ISOException e) {
            e.printStackTrace();
        } finally {
            System.out.println("--------------------");
        }

    }
}
