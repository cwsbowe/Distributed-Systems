import java.net.*;
import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;


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
        private HashMap<String, Integer> rebalanceCount;
        private ArrayList<Socket> storeSockets;
        private ArrayList<Socket> removeSockets;
        private int time;
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
                time = rebalance_period;
                timer = new Timer();
                timer.scheduleAtFixedRate(new TimerTask() {
                    @Override
                    public void run() {
                        time--;
                    }
                }, rebalance_period*1000, rebalance_period*1000);
                while (true) {
                    if (time <= 0) {
                        time = 0;
                        rebalance();
                    } else {
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
                }
            } catch (Exception e) {
                System.out.println(e);
            }
        }
        
        public void rebalance() {
            HashMap<Integer, ArrayList<String>> fileLists = new HashMap<>();
            rebalanceCount = new HashMap<>();
            try {
                for (int p : activePorts) {
                    Socket socket = new Socket();
                    socket.connect(new InetSocketAddress(InetAddress.getLocalHost(), p), timeout);
                    new PrintWriter(clientSocket.getOutputStream(), true).println("LIST");
                    fileLists.put(p, new ArrayList<>(Arrays.asList(new BufferedReader(new InputStreamReader(socket.getInputStream())).readLine().split(" "))));
                    fileLists.remove(0); //gets rid of "LIST"
                    socket.close();
                }
                HashMap<Integer, ArrayList<String>> newFileLists = new HashMap<>(fileLists);
                HashMap<String, ArrayList<Integer>> toSend = new HashMap<>();
                int val = R * filePorts.values().size() / activePorts.size();
                ArrayList<Integer> unbalanced = new ArrayList<>(newFileLists.keySet());
                ArrayList<Integer> fbalanced = new ArrayList<>();
                ArrayList<Integer> cbalanced = new ArrayList<>();
                ArrayList<Integer> unbalancedOver = new ArrayList<>();
                ArrayList<Integer> unbalancedUnder = new ArrayList<>();
                Boolean complete;
                for (String s : filePorts.keySet()) {
                    toSend.put(s, new ArrayList<>());
                    while (filePorts.get(s).size() < R) {
                        for (int p : activePorts) {
                            if (!filePorts.get(s).contains(p)) {
                                filePorts.get(s).add(p);
                                toSend.get(s).add(p);
                                break;
                            }
                        }
                    }
                }
                for (int p : unbalanced) {
                    if (newFileLists.get(p).size() == val) {
                        fbalanced.add(p);
                    } else if (newFileLists.get(p).size() == val+1) {
                        cbalanced.add(p);
                    } else if (newFileLists.get(p).size() < val) {
                        unbalancedUnder.add(p);
                    } else {
                        unbalancedOver.add(p);
                    }
                }
                while (unbalancedOver.size() > 0) {
                    for (int p : unbalancedOver) {
                        if (unbalancedUnder.size() > 0) {
                            complete = false;
                            for (int up : unbalancedUnder) {
                                for (String f : newFileLists.get(p)) {
                                    if (!newFileLists.get(up).contains(f)) {
                                        newFileLists.get(up).add(f);
                                        newFileLists.get(p).remove(f);
                                        toSend.get(f).add(up);
                                        if (newFileLists.get(up).size() == val) {
                                            unbalancedUnder.remove(up);
                                            
                                            fbalanced.add(up);
                                        }
                                        if (newFileLists.get(p).size() == val + 1) {
                                            unbalancedOver.remove(p);
                                            cbalanced.add(p);
                                            complete = true;
                                        }
                                        break;
                                    }
                                }
                                if (complete) {
                                    break;
                                }
                            }
                        } else {
                            complete = false;
                            for (int fp : fbalanced) {
                                for (String f : newFileLists.get(p)) {
                                    if (!newFileLists.get(fp).contains(f)) {
                                        newFileLists.get(fp).add(f);
                                        newFileLists.get(p).remove(f);
                                        toSend.get(f).add(fp);
                                        if (newFileLists.get(p).size() == val + 1) {
                                            unbalancedOver.remove(p);
                                            cbalanced.add(p);
                                            complete = true;
                                        }
                                        cbalanced.add(fp);
                                        fbalanced.remove(fp);
                                        break;
                                    }
                                }
                                if (complete) {
                                    break;
                                }
                            }
                        }
                    }
                }
                while (unbalancedUnder.size() > 0) {
                    for (int p : unbalancedUnder) {
                        complete = false;
                        for (int cp : cbalanced) {
                            for (String f : newFileLists.get(p)) {
                                if (newFileLists.get(cp).contains(f)) {
                                    newFileLists.get(cp).remove(f);
                                    newFileLists.get(p).add(f);
                                    toSend.get(f).add(p);
                                    if (newFileLists.get(p).size() == val) {
                                        unbalancedUnder.remove(p);
                                        fbalanced.add(p);
                                        complete = true;
                                    }
                                    cbalanced.remove(cp);
                                    fbalanced.add(cp);
                                    break;
                                }
                            }
                            if (complete) {
                                break;
                            }
                        }
                    }
                }
                for (int p : activePorts) {
                    String msg = "REBALANCE";
                    String msgToSend = "";
                    String msgToSendpt = "";
                    String msgToRemove = "";
                    int numFilesSend = 0;
                    int numFilesRemove = 0;
                    int numSend = 0;
                    int numRemove = 0;
                    for (String s : toSend.keySet()) {
                        if (fileLists.get(p).contains(s)) {
                            numFilesSend++;
                            msgToSend = msgToSend + " " + s;
                            for (int tp : toSend.get(s)) {
                                numSend++;
                                msgToSendpt = msgToSendpt + " " + tp;
                            }
                            msgToSend = msgToSend + " " + numSend + msgToSendpt;
                            msgToSendpt = "";
                            numSend = 0;
                        }
                    }
                    for (String s : fileLists.get(p)) {
                        if (!newFileLists.get(p).contains(s)) {
                            msgToRemove = msgToRemove + " " + s;
                            numFilesRemove++;
                        }
                    }
                    Socket socket = new Socket();
                    socket.connect(new InetSocketAddress(InetAddress.getLocalHost(), p), timeout);
                    PrintWriter pw = new PrintWriter(clientSocket.getOutputStream(), true);
                    BufferedReader br = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                    pw.println("REBALANCE " + numFilesSend + msgToSend + " " + numFilesRemove + msgToRemove);
                    if (br.readLine().equals("REBALANCE_COMPLETE")) {
                        pw.close();
                        br.close();
                        socket.close();
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