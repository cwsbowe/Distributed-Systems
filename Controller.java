import java.net.*;
import java.io.*;
import java.util.ArrayList;

public class Controller {
    private ServerSocket serverSocket;
    private Socket clientSocket;
    private ArrayList<Dstore> dstores;
    private int _numOfClients = 0;

    public void start (int cport, int R, int timeout, int rebalance_period) {
        dstores = new ArrayList<Dstore>();
        for (i=0; i<R; i++) {
            dstores.add(new Dstore())
        }
    }

    

    public void stop () {
        in.close();
        out.close();
        clientSocket.close();
        serverSocket.close();
    }

    public static void main (String[] args) throws Error{
        Controller cont = new Controller();
        try {
            cont.start(args[0], args[1], args[2], args[3]);
        } catch (Exception e) {
            System.out.println("Error, not enough arguments");
        }
    }


    private class contThread extends Thread {
        private Socket _clientSocket;
        private int port;

        contThread(Socket client) throws IOException {
            _clientSocket = client;
            
        }

    }

}