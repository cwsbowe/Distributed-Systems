import java.net.*;
import java.io.*;
import java.util.ArrayList;


public class Controller {
    private Socket contSocket;
    private InputStream in;
    private OutputStream out;
}


// public class Controller {
//     private ServerSocket serverSocket;
//     private Socket clientSocket;
//     private ArrayList<Dstore> dstores;
//     private int _numOfClients = 0;
//     private InputStream in;
//     private OutputStream out;

//     public void start (int cport, int R, int timeout, int rebalance_period) {
//         dstores = new ArrayList<Dstore>();
//         for (int i=0; i<R; i++) {
//             dstores.add(new Dstore());
//         }
//     }

//     private void run() {
//         try {
//             Socket s;
//             while (true) {
//                 s = serverSocket.accept();
//             }
//         } catch (Exception e) {
//             System.out.println(e);
//         }
//     }

//     public void stop () {
//         try {
//             in.close();
//             out.close();
//             clientSocket.close();
//             serverSocket.close();
//         } catch (Exception e) {
//             System.out.println(e);
//         }
//     }

//     public static void main (String[] args) throws Exception{
//         Controller cont = new Controller();
//         try {
//             cont.start(Integer.parseInt(args[0]), Integer.parseInt(args[1]), Integer.parseInt(args[2]), Integer.parseInt(args[3]));
//         } catch (Exception e) {
//             System.out.println("Error, not enough arguments");
//         }
//     }


//     private class contThread extends Thread {
//         private Socket _clientSocket;
//         private int port;

//         contThread(Socket client) throws IOException {
//             _clientSocket = client;
            
//         }

//     }

// }