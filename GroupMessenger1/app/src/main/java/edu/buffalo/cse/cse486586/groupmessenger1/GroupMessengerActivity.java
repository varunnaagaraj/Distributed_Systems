package edu.buffalo.cse.cse486586.groupmessenger1;

import android.annotation.SuppressLint;
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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * GroupMessengerActivity is the main Activity for the assignment.
 * 
 * @author stevko
 *
 */
public class GroupMessengerActivity extends Activity {

    static final String REMOTE_PORT_0 = "11108";
    static final String REMOTE_PORT_1 = "11112";
    static final String REMOTE_PORT_2 = "11116";
    static final String REMOTE_PORT_3 = "11120";
    static final String REMOTE_PORT_4 = "11124";

    static final String[] ports =
            new String[]{REMOTE_PORT_0, REMOTE_PORT_1, REMOTE_PORT_2, REMOTE_PORT_3, REMOTE_PORT_4};

    static int count_key_value = 0;
    final Uri providerUri = buildUri("content", "edu.buffalo.cse.cse486586.groupmessenger1.provider");

    private Uri buildUri(String content, String s) {
        Uri.Builder uri_builder = new Uri.Builder();
        uri_builder.authority(s);
        uri_builder.scheme(content);
        return uri_builder.build();
    }

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_group_messenger);
        TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));

        try{
            ServerSocket socket = new ServerSocket(10000);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, socket);
        }
        catch(IOException e){
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

    private class ServerTask extends AsyncTask<ServerSocket, String, Void>{
        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket socker = sockets[0];

            try{
                while (true) {
                    Socket socket = socker.accept();
                    DataInputStream input = new DataInputStream(socket.getInputStream());
                    // Intital message will be empty
                    String string_input = "";
                    // Read the input
                    string_input = input.readUTF();

                    // Call publishProgress with the input string to send the message to onProgressUpdate.
                    publishProgress(string_input);

                    // Close the Socket connection and DataInputStream connection
                    socket.close();
                    input.close();
                }
            }
            catch (Exception e) {
                Log.e("serverTask", "Error in Server task");
                e.printStackTrace();
            }
            return null;
        }

        protected void onProgressUpdate(String...strings) {
            /*
             * The following code displays what is received in doInBackground().
             */
            String strReceived = strings[0].trim();
            TextView remoteTextView = (TextView) findViewById(R.id.textView1);
            remoteTextView.append(strReceived + "\t\n");

            ContentValues map = new ContentValues();
            int length = 0;
            for (String s:fileList()){
                if (s.startsWith("groupmes")) length++;
            }
            map.put("key", Integer.toString(count_key_value));
            map.put("value", strReceived);
            count_key_value = count_key_value+1;


            Uri newUri = getContentResolver().insert(
                    providerUri, // assume we already created a Uri object with our provider URI
                    map
            );

        }

    }

    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            try {

                String newmessage = msgs[0];
                Socket s;

                for (String sock : ports) {
                    s = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(sock));
                    OutputStream os = s.getOutputStream();
                    DataOutputStream out = new DataOutputStream(os);
                    out.writeUTF(newmessage);
                }
            } catch (Exception e){
                Log.e("ClientTask", "Error in Client task");
                e.printStackTrace();

            }
            return null;
        }
    }
}
