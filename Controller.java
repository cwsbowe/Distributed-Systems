import java.net.*;
import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Timer;
import java.util.TimerTask;


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
        private HashMap<String, Integer> removeCount;
        private ArrayList<Socket> storeSockets;
        private ArrayList<Socket> removeSockets;
        private Timer timer;
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
                timer = new Timer();
                timer.scheduleAtFixedRate(new TimerTask() {
                    @Override
                    public void run() {
                        rebalance();
                    }
                }, rebalance_period*1000, rebalance_period*1000);
                while (true) {
                    nextLine = reader.readLine().split(" ");
                    if (nextLine[0].equals("JOIN")) {
                        activePorts.add(Integer.parseInt(nextLine[1]));
                        rebalance();
                    } else if (nextLine[0].equals("STORE")) {
                        ArrayList<Integer> ports = new ArrayList<>();
                        if (R < activePorts.size()) {
                            for (int i=0; i < R; i++) {
                                ports.add(activePorts.get(i));
                            }
                            filePorts.put(nextLine[1], ports);
                            fileSizes.put(nextLine[1], Integer.parseInt(nextLine[2]));
                            storeCount.put(nextLine[1], 0);
                            String s = "STORE_TO";
                            storeSockets = new ArrayList<>();
                            for (int i=0; i < ports.size(); i++) {
                                s = s + " " + ports.get(i);
                                Socket storeSocket = new Socket();
                                storeSocket.connect(new InetSocketAddress(InetAddress.getLocalHost(), ports.get(i)), timeout);
                                storeSockets.add(storeSocket);
                            }
                            writer.println(s);
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
                    } else if (nextLine[0].equals("REMOVE") && filePorts.containsKey(nextLine[1])) {
                        removeCount.put(nextLine[1], 0);
                        removeSockets = new ArrayList<>();
                        for (int n : filePorts.get(nextLine[1])) {
                            Socket removeSocket = new Socket();
                            removeSocket.connect(new InetSocketAddress(InetAddress.getLocalHost(), n), timeout);
                            removeSockets.add(removeSocket);
                            new PrintWriter (removeSockets.get(removeSockets.size()-1).getOutputStream(), true).println("REMOVE " + nextLine[1]);
                        }
                    } else if (nextLine[0].equals("REMOVE")) {
                        writer.println("ERROR_FILE_DOES_NOT_EXIST");
                    } else if (nextLine[0].equals("LIST")) {
                        String s = "LIST";
                        for (String f : filePorts.keySet()) {
                            s = s + " " + f;
                        }
                        writer.println(s);
                    } else if (nextLine[0].equals("STORE_ACK")) {
                        if (storeCount.get(nextLine[1]) == R-1) {
                            writer.println("STORE_COMPLETE");
                            storeCount.remove(nextLine[1]); //might be unnecessary
                        } else {
                            storeCount.put(nextLine[1], storeCount.get(nextLine[1]) + 1);
                        }
                    } else if (nextLine[0].equals("REMOVE_ACK")) {
                        if (removeCount.get(nextLine[1]) == R-1) {
                            writer.println("REMOVE_COMPLETE");
                            removeCount.remove(nextLine[1]); //might be unnecessary
                        } else {
                            removeCount.put(nextLine[1], removeCount.get(nextLine[1]) + 1);
                        }
                    }
                }
            } catch (Exception e) {
                System.out.println(e);
            }
        }
        
        public void rebalance() {
            HashMap<Integer, String[]> fileLists = new HashMap<>();
            try {
                for (int p : activePorts) {
                    Socket socket = new Socket();
                    socket.connect(new InetSocketAddress(InetAddress.getLocalHost(), p), timeout);
                    new PrintWriter(clientSocket.getOutputStream(), true).println("LIST");
                    fileLists.put(p, new BufferedReader(new InputStreamReader(socket.getInputStream())).readLine().split(" "));
                    socket.close();
                }
                //todo sort fileLists
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