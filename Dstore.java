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
        private String[] nextLine;
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
            try {
                controllerSocket = new Socket(InetAddress.getLocalHost(), cport); //connects to controller
                instream = socket.getInputStream();
                outstream = socket.getOutputStream();
                fromClient = new BufferedReader(new InputStreamReader(instream));
                toClient = new PrintWriter(outstream, true);
                toCont = new PrintWriter(controllerSocket.getOutputStream(), true);
                while (true) {
                    nextLine = fromClient.readLine().split(" ");
                    if (nextLine[0].equals("STORE")) {
                        file = new File(file_folder + nextLine[1]);
                        if (file.exists()) {
                            toCont.println("ERROR_FILE_ALREADY_EXISTS"); //controller sends this to client
                        } else {
                            toClient.println("ACK");
                        }
                        file.createNewFile();
                        FileOutputStream fos = new FileOutputStream(file);
                        fos.write(instream.readNBytes(Integer.parseInt(nextLine[2])));
                        fos.close();
                        toCont.println("STORE_ACK " + nextLine[1]);
                    } else if (nextLine[0].equals("LOAD_DATA")) {
                        file = new File(file_folder + nextLine[1]);
                        if (file.exists()) {
                            FileInputStream fis = new FileInputStream(file);
                            outstream.write(fis.read());
                            fis.close();
                        } else {
                            halt();
                        }
                    } else if (nextLine[0].equals("REMOVE")) {
                        file = new File(file_folder + nextLine[1]);
                        if (file.exists()) {
                            file.delete();
                            toClient.println("REMOVE_ACK " + nextLine[1]);
                        } else {
                            toClient.println("ERROR_FILE_DOES_NOT_EXIST " + nextLine[1]);
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