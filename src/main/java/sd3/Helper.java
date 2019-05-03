package sd3;

import java.io.*;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.text.DateFormat;
import java.util.Date;

public class Helper {
    private static final boolean DEBUG = false;
    private static int MAX_TRIES = 10;
    private static int RETRY_WAIT = 1000;

    public static String sendRequest(InetSocketAddress server, String req) {
        if (server == null || req == null) {
            throw new IllegalArgumentException("sendRequest got bogus server or request for " + req);
            //System.out.println("null server or request");
            //return null;
        }

        Socket talkSocket = null;

        if (DEBUG)
            System.out.println("At " + DateFormat.getInstance().format(new Date()) + ": About to send " + req + " to " + server);
        int tries = 0;
        do {
            if (tries > 0) {
                try {
                    Thread.sleep(RETRY_WAIT);
                } catch (InterruptedException e) { /* ignore */ }
            }
            try {
                talkSocket = new Socket(server.getAddress(), server.getPort());
                PrintStream output = new PrintStream(talkSocket.getOutputStream());
                output.println(req);
            } catch (IOException e) {
                System.err.println("Error trying to connect to " + server);
                e.printStackTrace();
            }
            ++tries;
        } while (talkSocket == null && tries < 10);
        if (talkSocket == null) {
            System.err.println("gave up on contacting " + server + " to send " + req);
            return null;
        }
        try {
            Thread.sleep(60);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        InputStream input = null;
        try {
            input = talkSocket.getInputStream();
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Cannot get input stream from " + server.toString() + "\nRequest is: " + req + "\n");
        }
        String response = Helper.inputStreamToString(input);
        try {
            talkSocket.close();
        } catch (IOException e) {
            System.out.println("cannot close socket");
            e.printStackTrace();
        }
        return response;
    }

    public static InetSocketAddress createSocketAddress(String addr) {

        // input null, return null
        if (addr == null) {
            return null;
        }

        // split input into ip string and port string
        String[] splitted = addr.split(":");

        // can split string
        if (splitted.length >= 2) {

            //get and pre-process ip address string
            String ip = splitted[0];
            if (ip.startsWith("/")) {
                ip = ip.substring(1);
            }

            //parse ip address, if fail, return null
            InetAddress m_ip = null;
            try {
                m_ip = InetAddress.getByName(ip);
            } catch (UnknownHostException e) {
                System.out.println("Cannot create ip address: " + ip);
                return null;
            }

            // parse port number
            String port = splitted[1];
            int m_port = Integer.parseInt(port);

            // combine ip addr and port in socket address
            return new InetSocketAddress(m_ip, m_port);
        }

        // cannot split string
        else {
            return null;
        }

    }

    public static String inputStreamToString(InputStream in) {

        // invalid input
        if (in == null) {
            return null;
        }

        // try to read line from input stream
        BufferedReader reader = new BufferedReader(new InputStreamReader(in));
        String line = null;
        try {
            line = reader.readLine();
        } catch (IOException e) {
            System.out.println("Cannot read line from input stream.");
            return null;
        }

        return line;
    }
}

