import java.net.*;
import java.io.*;
import java.util.ArrayList;

public class Dstore {
    private ServerSocket serverSocket;
    private Socket socket;
    private Socket controllerSocket;
    private BufferedReader fromClient;
    private PrintWriter toClient;
    private BufferedReader fromCont;
    private PrintWriter toCont;
    private String[] fstLine;
    private Boolean sto;
    private ArrayList<String> temps;
    private File file;


    public void start(int port, int cport, int timeout, String file_folder) {
        sto = false;
        temps = new ArrayList<String>();
        try {
            serverSocket = new ServerSocket(port); //connects to client
            controllerSocket = new Socket(InetAddress.getLocalHost(), cport); //connects to controller
            while (true) {
                socket = serverSocket.accept(); //waits for client to send something
                fromClient = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                toClient = new PrintWriter(socket.getOutputStream(), true);
                if (sto) {
                    file.createNewFile();
                    FileWriter w = new FileWriter(file);
                    String line;
                    while ((line = fromClient.readLine()) != null) {
                        w.write(line);
                    }
                    w.close();
                    toCont = new PrintWriter(controllerSocket.getOutputStream(), true);
                    toCont.println("STORE_ACK " + temps.get(0));
                } else {
                    fstLine = fromClient.readLine().split(" ");
                    if (fstLine[0].equals("STORE")) {
                        file = new File(fstLine[1]);
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
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    public void stop() {
        try {
            fromClient.close();
            toClient.close();
        } catch (Exception e) {
            System.out.println(e);
        }
    }  

    public static void main(String[] args) {
        Dstore ds = new Dstore();
        try {
            ds.start(Integer.parseInt(args[0]), Integer.parseInt(args[1]), Integer.parseInt(args[2]), args[3]);
        } catch (Exception e) {
            System.out.println(e);
        }
    }
}