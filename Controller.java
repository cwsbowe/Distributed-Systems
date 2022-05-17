import java.net.*;
import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;


public class Controller {
    private ServerSocket contSocket;

    public void start(int cport, int R, int timeout, int rebalance_period) {
        try {
            contSocket = new ServerSocket(cport);
            contSocket.setSoTimeout(timeout);
            while (true) {
                new EchoController(contSocket.accept(), cport, R, timeout, rebalance_period).start();
            }
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    private static class EchoController extends Thread { //removed static, maybe broke idk
        private Socket clientSocket;
        private BufferedReader reader;
        private PrintWriter writer;
        private ArrayList<Integer> activePorts;
        private HashMap<String, Integer> filePorts;
        private HashMap<String, Integer> storeCount;
        private String[] nextLine;
        private int cport;
        private int R;
        private int timeout;
        private int rebalance_period;


        public EchoController(Socket socket, int cport, int R, int timeout, int rebalance_period) {
            this.clientSocket = socket;
            this.cport = cport;
            this.R = R;
            this.timeout = timeout;
            this.rebalance_period = rebalance_period;

        }
        
        public void run() {
            try {
                reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                writer = new PrintWriter(clientSocket.getOutputStream(), true);
                activePorts = new ArrayList<>();
                filePorts = new HashMap<>();
                storeCount = new HashMap<>();
                while (true) {
                    nextLine = reader.readLine().split(" ");
                    if (nextLine[0].equals("JOIN")) {
                        activePorts.add(Integer.parseInt(nextLine[1]));
                        writer.println("LIST");
                    } else if (nextLine[0].equals("STORE")) {
                        ArrayList<Integer> ports = new ArrayList<>();
                        int p;
                        if (R < activePorts.size()) {
                            for (int i=0; i < R; i++) {
                                p = activePorts.get(i);
                                ports.add(p);
                                filePorts.put(nextLine[1], p);
                            }
                            storeCount.put(nextLine[1], 0);
                            writer.println("STORE_TO");
                            for (int i=0; i < ports.size(); i++) {
                                writer.print(" " + ports.get(i));
                            }
                        } else {
                            writer.println("ERROR_NOT_ENOUGH_DSTORES");
                        }
                    } else if (nextLine[0].equals("STORE_ACK")) {
                        if (storeCount.get(nextLine[1]) == R-1) {
                            writer.println("STORE_COMPLETE");
                            storeCount.remove(nextLine[1]); //might be unnecessary
                        } else {
                            storeCount.put(nextLine[1], storeCount.get(nextLine[1]) + 1);
                        }
                    }
                }
            } catch (Exception e) {
                System.out.println(e);
            }
        }

    }

    public static void main(String[] args) {
        Controller cont = new Controller();
        try {
            cont.start(Integer.parseInt(args[0]), Integer.parseInt(args[1]), Integer.parseInt(args[2]), Integer.parseInt(args[3]));
        } catch (Exception e) {
            System.out.println(e); //either not enough args or args arent ints
        }
    }
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