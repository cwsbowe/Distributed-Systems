import java.net.*;
import java.io.*;
import java.util.ArrayList;

public class Dstore {
    private ServerSocket serverSocket;

    public void start(int port, int cport, int timeout, String file_folder) {
        try {
            serverSocket = new ServerSocket(port);
            serverSocket.setSoTimeout(timeout);
            while (true) {
                new EchoDstore(serverSocket.accept(), port, cport, timeout, file_folder).start();
            }
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    // public void stop() {
    //     try {
    //         fromClient.close();
    //         toClient.close();
    //     } catch (Exception e) {
    //         System.out.println(e);
    //     }
    // }

    private static class EchoDstore extends Thread{
        private ServerSocket serverSocket;
        private Socket socket;
        private Socket controllerSocket;
        private InputStream instream;
        private OutputStream outstream;
        private BufferedReader fromClient;
        private PrintWriter toClient;
        private BufferedReader fromCont;
        private PrintWriter toCont;
        private String[] fstLine;
        private Boolean sto;
        private ArrayList<String> temps;
        private File file;
        private int port;
        private int cport;
        private int timeout;
        private String file_folder;

        public EchoDstore(Socket socket, int port, int cport, int timeout, String file_folder) {
            this.socket = socket;
            this.port = port;
            this.cport = cport;
            this.timeout = timeout;
            this.file_folder = file_folder;
        }

        public void run() {
            sto = false;
            temps = new ArrayList<>();
            try {
                controllerSocket = new Socket(InetAddress.getLocalHost(), cport); //connects to controller
                instream = socket.getInputStream();
                outstream = socket.getOutputStream();
                fromClient = new BufferedReader(new InputStreamReader(instream));
                toClient = new PrintWriter(outstream, true);
                while (true) {
                    if (sto) {
                        sto = false;
                        file.createNewFile();
                        FileOutputStream fos = new FileOutputStream(file);
                        fos.write(instream.readNBytes(Integer.parseInt(temps.get(1))));
                        fos.close();
                        toCont = new PrintWriter(controllerSocket.getOutputStream(), true);
                        toCont.println("STORE_ACK " + temps.get(0));
                        temps = new ArrayList<String>();
                    } else {
                        fstLine = fromClient.readLine().split(" ");
                        if (fstLine[0].equals("STORE")) {
                            file = new File(file_folder + fstLine[1]);
                            if (file.exists()) {
                                toCont.println("ERROR_FILE_ALREADY_EXISTS"); //controller sends this to client
                            } else {
                                toClient.println("ACK");
                                sto = true;
                                temps.add(fstLine[1]);
                                temps.add(fstLine[2]);
                            }
                        }
                    }
                }
            } catch (SocketTimeoutException e) {
                halt();
            } catch (Exception e) {
                System.out.println(e);
            }
        }

        public void halt() {
            try {
                fromClient.close();
                toClient.close();
                socket.close();
                controllerSocket.close();
            } catch (Exception e) {
                System.out.println(e);
            }
        }

    }

    public static void main(String[] args) {
        Dstore ds = new Dstore();
        try {
            ds.start(Integer.parseInt(args[0]), Integer.parseInt(args[1]), Integer.parseInt(args[2]), args[3]);
        } catch (Exception e) {
            System.out.println(e); //either not enough args or args arent ints
        }
    }
}