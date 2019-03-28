package edu.buffalo.cse.cse486586.groupmessenger2;

import android.app.Activity;
import android.content.ContentValues;
import android.content.Context;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Bundle;
import android.telephony.TelephonyManager;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.widget.EditText;
import android.widget.TextView;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.concurrent.PriorityBlockingQueue;

/**
 * GroupMessengerActivity is the main Activity for the assignment.
 *
 * @author stevko
 */
public class GroupMessengerActivity extends Activity {

    static final String REMOTE_PORT_0 = "11108";
    static final String REMOTE_PORT_1 = "11112";
    static final String REMOTE_PORT_2 = "11116";
    static final String REMOTE_PORT_3 = "11120";
    static final String REMOTE_PORT_4 = "11124";
    static PriorityBlockingQueue<String> holdback_queue = new PriorityBlockingQueue<String>(30, new HBQComparator());
    static HashMap<String, LinkedHashSet<String>> proposal_tracker = new HashMap<String, LinkedHashSet<String>>(30);
    final Uri providerUri = buildUri("content", "edu.buffalo.cse.cse486586.groupmessenger2.provider");
    ArrayList<String> ports = new ArrayList<String>();
    int seq = 0, count = 0, device_id;
    int count_key_value = 0;
    int failed_node = 0;

    private Uri buildUri(String content, String s) {
        Uri.Builder uri_builder = new Uri.Builder();
        uri_builder.authority(s);
        uri_builder.scheme(content);
        return uri_builder.build();
    }

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        ports.add(REMOTE_PORT_0);
        ports.add(REMOTE_PORT_1);
        ports.add(REMOTE_PORT_2);
        ports.add(REMOTE_PORT_3);
        ports.add(REMOTE_PORT_4);
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_group_messenger);
        TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        device_id = Integer.parseInt(myPort);

        try {
            ServerSocket socket = new ServerSocket(10000);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, socket);
        } catch (IOException e) {
            Log.e("SocketError", "Failed during socket creation");
            e.printStackTrace();
            return;
        }
        /*
         * TODO: Use the TextView to display your messages. Though there is no grading component
         * on how you display the messages, if you implement it, it'll make your debugging easier.
         *
         */
        TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());

        /*
         * Registers OnPTestClickListener for "button1" in the layout, which is the "PTest" button.
         * OnPTestClickListener demonstrates how to access a ContentProvider.
         */
        findViewById(R.id.button1).setOnClickListener(
                new OnPTestClickListener(tv, getContentResolver()));

        /*
         * TODO: You need to register and implement an OnClickListener for the "Send" button.
         * In your implementation you need to get the message from the input box (EditText)
         * and send it to other AVDs.
         */

        final EditText edittext = (EditText) findViewById(R.id.editText1);

        findViewById(R.id.button4).setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                String message = edittext.getText().toString();
                edittext.setText("");
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message, myPort);
            }
        });
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_group_messenger, menu);
        return true;
    }

    // ISIS Algorithm Reference: https://studylib.net/doc/7830646/isis-algorithm-for-total-ordering-of-messages
    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serversock = sockets[0];
            try {
                while (true) {
                    Socket socket = serversock.accept();
                    DataInputStream input = new DataInputStream(socket.getInputStream());
                    String incoming_message = input.readUTF();
                    String[] details = incoming_message.split(":");
                    // 4th element has the source port
                    Log.d("Incoming Message", incoming_message);

                    if (details[0].equals("initial")) {
                        seq += 1;
                        DataOutputStream output = new DataOutputStream(socket.getOutputStream());
                        Log.d("Prev Agreed", String.valueOf(seq));
                        Log.d("Actual Agreed", String.valueOf(Math.max(seq, Integer.parseInt(details[1]))));
                        String proposal_to_send = "proposal" + ":" + details[2] + ":" + seq + ":" + details[3] + ":" + details[4];
                        output.writeUTF(proposal_to_send);
                        String message_to_add_to_q = "initial" + ":" + seq + ":" + details[2] + ":" + details[3] + ":" + details[4] + ":" + details[5] + ":" + "0";
                        holdback_queue.add(message_to_add_to_q);
                    }

                    if (details[0].equals("final")) {
                        if (Integer.parseInt(details[1]) > seq) {
                            seq = Integer.parseInt(details[1]);
                        }


                        Log.d("proposal trackers", String.valueOf(proposal_tracker));
                        //Reference: Iterating over a Priority Queue https://stackoverflow.com/a/25850478
                        for (String msg : holdback_queue) {
                            Log.d("Message", msg);
                            Log.d("Details", incoming_message);
                            if (msg.split(":")[3].equals(details[3]) && msg.split(":")[4].equals(details[4])) {
                                Log.d("Splitting", msg);
                                holdback_queue.remove(msg);
                                String temp = "final" + ":" + details[1] + ":" + details[2] + ":" + details[3] + ":" + details[4] + ":" + details[5] + ":" + "1";
                                Log.d("Adding to Q", temp);
                                holdback_queue.add(temp);
                            }
                        }
                    }

                    for (String msg : holdback_queue) {
                        Log.d("Head of queue", msg);
                    }
                    Log.d("Failed node is", String.valueOf(failed_node));
                    for (String msg : holdback_queue) {
                        if (failed_node == Integer.parseInt(msg.split(":")[3])) {
                            holdback_queue.remove(msg);
                        }
                    }
                    for (String msg : holdback_queue) {
                        Log.d("queue after removals", msg);
                    }

                    while (holdback_queue.size() != 0) {
                        if (holdback_queue.peek().split(":")[6].equals("1")) {
                            Log.d("Publishing ", holdback_queue.peek());
                            publishProgress(holdback_queue.poll().split(":")[4]);
                            if (holdback_queue.size() == 0)
                                break;
                        } else
                            break;

                    }
                }
            } catch (NullPointerException npe) {
                Log.e("NPE", "Null Pointer Exception on Peek");
                npe.printStackTrace();
            } catch (Exception e) {
                Log.e("General", "Exception caught on Server side");
                e.printStackTrace();
            }
            return null;
        }

        protected void onProgressUpdate(String... strings) {
            /*
             * The following code displays what is received in doInBackground().
             */
            String strReceived = strings[0].trim();
            TextView remoteTextView = (TextView) findViewById(R.id.textView1);
            remoteTextView.append(strReceived + "\t\n");

            ContentValues map = new ContentValues();
            int length = 0;
            for (String s : fileList()) {
                if (s.startsWith("groupmes")) length++;
            }
            map.put("key", count_key_value);
            map.put("value", strReceived);
            count_key_value = count_key_value + 1;


            Uri newUri = getContentResolver().insert(
                    providerUri, // assume we already created a Uri object with our provider URI
                    map
            );
        }

    }

    // ISIS Algorithm Reference: https://studylib.net/doc/7830646/isis-algorithm-for-total-ordering-of-messages
    private class ClientTask extends AsyncTask<String, Void, Void> {
        @Override
        protected Void doInBackground(String... msgs) {
            // Reference: https://stackify.com/specify-handle-exceptions-java/
            // Keep track of the proposals in a ArrayList.
            ArrayList<String> proposal_list = new ArrayList<String>();
            count = count + 1;
            String seq_port = String.valueOf(count) + msgs[1];
            if (failed_node != 0)
                ports.remove(String.valueOf(failed_node));
            Log.d("Ports are ", String.valueOf(ports));
            // Sending the initial proposals to all the ports
            for (String port : ports) {
                try {
                    Log.d("Port to send", port);
                    Log.d("Failed port", String.valueOf(failed_node));
                    if (Integer.parseInt(port) == failed_node)
                        continue;
                    String proposal = null;

                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port));
                    // Set Socket Timeout to detect failed ports when we dont get response
                    socket.setSoTimeout(3000);
                    String initial_message = "initial" + ":" + count + ":" + seq_port + ":" + msgs[1] + ":" + msgs[0] + ":" + String.valueOf(failed_node);
                    Log.d("Source port is ", msgs[1]);

                    Log.d("Initial Message", initial_message);
                    DataOutputStream output = new DataOutputStream(socket.getOutputStream());
                    DataInputStream input = new DataInputStream(socket.getInputStream());
                    output.writeUTF(initial_message);
                    // Might throw IOException in case the AVD is down. Catch it and update failed node
                    proposal = input.readUTF();
                    if (proposal != null)
                        proposal_list.add(proposal);
                } catch (IOException io) {
                    io.printStackTrace();
                    Log.e("Error", "Unresponsive Node caught");
                    failed_node = Integer.parseInt(port);
                }
            }
            // Reference:  https://stackoverflow.com/questions/13056178/java-sorting-an-string-array-by-a-substring-of-characters
            Collections.sort(proposal_list, new StringComparator());
            int size = proposal_list.size();
            Log.d("Proposal List", String.valueOf(proposal_list));
            String proposal = proposal_list.get(size - 1).split(":")[2];
            Log.d("Proposal", proposal_list.get(size - 1));
            String port_source = proposal_list.get(size - 1).split(":")[3];
            try {
                // https://www.journaldev.com/1020/thread-sleep-java#how-thread-sleep-works
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            // Sending the final agreements to all the ports
            for (String port : ports) {
                try {
                    if (Integer.parseInt(port) == failed_node)
                        continue;
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port));
                    // Changed the timeout after talking to a friend who mentioned Steve Ko suggested 2000-3000ms timeout
                    socket.setSoTimeout(3000);
                    String final_message_to_broadcast = "final" + ":" + proposal + ":" + seq_port + ":" + port_source + ":" + msgs[0] + ":" + String.valueOf(failed_node) + ":" + "0";
                    DataOutputStream output = new DataOutputStream(socket.getOutputStream());
                    output.writeUTF(final_message_to_broadcast);
                    socket.close();
                    Log.d("Broadcasted", final_message_to_broadcast);
                } catch (IOException io) {
                    Log.e("IO", "IO exception at Client socket");
                    failed_node = Integer.parseInt(port);
                    io.printStackTrace();
                } catch (Exception e) {
                    Log.e("ClientTask", "Error in Client task");
                    e.printStackTrace();
                }
            }
            try {
                //https://www.journaldev.com/1020/thread-sleep-java#how-thread-sleep-works
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return null;
        }
    }
}

// Reference : https://www.geeksforgeeks.org/implement-priorityqueue-comparator-java/
class StringComparator implements java.util.Comparator<String> {
    /* This class is used for comparing the elements present in the Proposal List*/
    @Override
    public int compare(String s1, String s2) {
        if (Integer.parseInt(s1.split(":")[2]) > Integer.parseInt(s2.split(":")[2])) {
            return 1;
        } else if (Integer.parseInt(s1.split(":")[2]) < Integer.parseInt(s2.split(":")[2])) {
            return -1;
        } else {
            if (Integer.parseInt(s1.split(":")[3]) < Integer.parseInt(s2.split(":")[3])) {
                return 1;
            } else {
                return -1;
            }
        }
    }
}

// Reference : https://www.geeksforgeeks.org/implement-priorityqueue-comparator-java/
class HBQComparator implements java.util.Comparator<String> {
    /* This class is used for comparing the elements present in the Holdback Queue*/
    @Override
    public int compare(String s1, String s2) {
        if (Integer.parseInt(s1.split(":")[1]) > Integer.parseInt(s2.split(":")[1])) {
            return 1;
        } else if (Integer.parseInt(s1.split(":")[1]) < Integer.parseInt(s2.split(":")[1])) {
            return -1;
        } else {
            if (Integer.parseInt(s1.split(":")[3]) < Integer.parseInt(s2.split(":")[3])) {
                return -1;
            } else {
                return 1;
            }
        }
    }
}