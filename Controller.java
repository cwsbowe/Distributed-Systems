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
        private HashMap<String, ArrayList<Integer>> filePorts;
        private HashMap<String, Integer> fileSizes;
        private HashMap<String, Integer> storeCount;
        private String[] nextLine;
        private int loadAttempt;
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
                fileSizes = new HashMap<>();
                storeCount = new HashMap<>();
                while (true) {
                    nextLine = reader.readLine().split(" ");
                    if (nextLine[0].equals("JOIN")) {
                        activePorts.add(Integer.parseInt(nextLine[1]));
                        writer.println("LIST");
                    } else if (nextLine[0].equals("STORE")) {
                        ArrayList<Integer> ports = new ArrayList<>();
                        if (R < activePorts.size()) {
                            for (int i=0; i < R; i++) {
                                ports.add(activePorts.get(i));
                            }
                            filePorts.put(nextLine[1], ports);
                            fileSizes.put(nextLine[1], Integer.parseInt(nextLine[2]));
                            storeCount.put(nextLine[1], 0);
                            writer.println("STORE_TO");
                            for (int i=0; i < ports.size(); i++) {
                                writer.print(" " + ports.get(i));
                            }
                        } else {
                            writer.println("ERROR_NOT_ENOUGH_DSTORES");
                        }
                    } else if (nextLine[0].equals("LOAD")) {
                        writer.println("LOAD_FROM " + filePorts.get(nextLine[1]).get(0) + " " + fileSizes.get(nextLine[1]));
                        loadAttempt = 0;
                    } else if (nextLine[0].equals("RELOAD")) {
                        if (loadAttempt < R-1 && loadAttempt < activePorts.size()) {
                            writer.println("LOAD_FROM " + filePorts.get(nextLine[1]).get(loadAttempt) + " " + fileSizes.get(nextLine[1]));
                            loadAttempt++;
                        } else if (loadAttempt < R-1) {
                            writer.println("ERROR_NOT_ENOUGH_DSTORES");
                        } else {
                            writer.println("ERROR_FILE_DOES_NOT_EXIST"); //idk when its supposed to do ERROR_LOAD
                        }
                    } else if (nextLine[0].equals("REMOVE")) {
                        //
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