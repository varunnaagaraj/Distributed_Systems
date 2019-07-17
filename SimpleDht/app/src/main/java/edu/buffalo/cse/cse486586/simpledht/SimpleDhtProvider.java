package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.concurrent.ExecutionException;

public class SimpleDhtProvider extends ContentProvider {
    static final String REMOTE_PORT_0 = "11108";
    static final String REMOTE_PORT_1 = "11112";
    static final String REMOTE_PORT_2 = "11116";
    static final String REMOTE_PORT_3 = "11120";
    static final String REMOTE_PORT_4 = "11124";

    static final String[] ports =
            new String[]{REMOTE_PORT_0, REMOTE_PORT_1, REMOTE_PORT_2, REMOTE_PORT_3, REMOTE_PORT_4};

    final Uri providerUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");

    private Uri buildUri(String content, String s) {
        Uri.Builder uri_builder = new Uri.Builder();
        uri_builder.authority(s);
        uri_builder.scheme(content);
        return uri_builder.build();
    }

    String current_node_hash, current_port;
    HashMap<String, String> node_hash_value = new HashMap<String, String>();
    ArrayList<String> nodes_list = new ArrayList<String>();
    String s_hash = null, p_hash = null;
    int s_port = 0, p_port = 0;


    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        String[] list_of_files = getContext().getFilesDir().list();
        if ((s_port == 0 && p_port == 0) || selection.equals("@")) {
            delete_files(list_of_files);
        } else if (selection.startsWith("*") && selection.length() == 5) {
            if (!selection.contains(String.valueOf(s_port))) {
                delete_files(list_of_files);
            }
            delete_files(list_of_files);
        } else if (selection.equals("*")) {
            String forward_delete = "delete" + ":" + s_port + ":" + selection;
            Log.d("Forward Delete", forward_delete);
            String response = null;
            try {
                response = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, forward_delete).get();
                Log.d("Response from * del", response);
                assert response.equals("DONE");
                delete_files(list_of_files);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        } else if (selection.length() >= 6) {
            for (String file : list_of_files) {
                if (selection.contains(file)) {
                    getContext().deleteFile(file);
                }
            }
        } else {
            String forward_delete = "delete" + ":" + s_port + ":" + selection;
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, forward_delete);
        }
        return 0;
    }

    private void delete_files(String[] list_of_files) {
        // Reference : https://developer.android.com/reference/android/content/Context#deleteFile(java.lang.String)
        for (String file : list_of_files) {
            getContext().deleteFile(file);
        }
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        String value_hash = "";
        try {
            value_hash = genHash(values.getAsString("key"));
            String key = values.getAsString("key");
            String val = values.getAsString("value");
            Log.d("Key to Insert", key);
            Log.d("Key Hash", value_hash);
            Log.d("Current Hash", current_node_hash);
            // Reference : https://piazza.com/class/jrghj69reaw4oo?cid=454 answer
            if ((p_port == 0 && s_port == 0) || (value_hash.compareTo(p_hash) > 0 && value_hash.compareTo(current_node_hash) <= 0) ||
                    ((current_node_hash.compareTo(p_hash) < 0 && current_node_hash.compareTo(s_hash) < 0)
                    && (value_hash.compareTo(p_hash) > 0 || value_hash.compareTo(current_node_hash) <= 0))) {
                Log.d("Inserted Locally", current_port + ":" + key);
                write_file(key, val);
            } else {
                Log.d("Forwarding to ", String.valueOf(s_port));
                String forward_to_successor = "insert" + ":" + s_port + ":" + key + ":" + val;
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, forward_to_successor, String.valueOf(s_port * 2));
            }
        } catch (NoSuchAlgorithmException nsae) {
            nsae.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private void write_file(String key, String val) throws IOException {
        FileOutputStream fileOutputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);
        fileOutputStream.write(val.getBytes());
        fileOutputStream.close();
    }


    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
        TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String myPort = String.valueOf(Integer.parseInt(portStr) * 2);

        ServerSocket socket;
        try {
            socket = new ServerSocket(10000);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, socket);
        } catch (IOException e) {
            Log.e("SocketError", "Failed during socket creation");
            e.printStackTrace();
            return false;
        }

        current_port = portStr;
        Log.d("Current port OnCreate", current_port);
        try {
            current_node_hash = genHash(current_port);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        try {
            String temp = String.valueOf("5554");
            node_hash_value.put(genHash(temp), temp);
            temp = String.valueOf("5556");
            node_hash_value.put(genHash(temp), temp);
            temp = String.valueOf("5558");
            node_hash_value.put(genHash(temp), temp);
            temp = String.valueOf("5560");
            node_hash_value.put(genHash(temp), temp);
            temp = String.valueOf("5562");
            node_hash_value.put(genHash(temp), temp);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        Log.d("Node Hash Values", String.valueOf(node_hash_value));


        if (!current_port.equals("5554")) {
            String join = "join" + ":" + current_port;
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, join);
        } else {
            try {
                nodes_list.add(genHash(current_port));
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
        }
        return false;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        // TODO Auto-generated method stub
        Log.d("Querying", selection);
        Log.d("---------", "\n");
        String message = "";
        Context context = getContext();
        MatrixCursor cursor = new MatrixCursor(new String[]{"key", "value"});
        Log.d("CURSOR", cursor.toString());
        String[] list_of_files = context.getFilesDir().list();
        Log.d("FileList", Arrays.toString(list_of_files));
        String value_hash = "";
        try {
            value_hash = genHash(selection);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        Log.d("Value Hash", value_hash);
        cursor.moveToFirst();
        if ((p_port == 0 && s_port == 0) || selection.equals("@")) {
            if (selection.length() == 1) {
                for (String file : list_of_files) {
                    try {
                        query_file(file, cursor);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                return cursor;
            } else {
                try {
                    query_file(selection, cursor);
                    return cursor;
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } else if (selection.contains("*") && selection.length() == 5) {
            try {
                Log.d("Substring", selection.substring(1));
                if (!selection.contains(String.valueOf(s_port))) {
                    Log.d("Inside Successor Query", String.valueOf(s_port));
                    String forward_query = "query" + ":" + s_port + ":" + selection;
                    Log.d("Forward Query succ", forward_query);
                    String response = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, forward_query).get();
                    Log.d("Response", response);
                    response = response.replaceAll("\\{", "");
                    response = response.replaceAll("\\}", "");
                    String key_value[] = response.split(",");
                    Log.d("Keyvalue", Arrays.toString(key_value));
                    int count = 0;
                    for (String values : key_value) {
                        String key = "";
                        String val = "";
                        try {
                            key = values.split("=")[0];
                            val = values.split("=")[1];
                        } catch (ArrayIndexOutOfBoundsException arr) {
                            Log.d("Error in array call", arr.toString());
                        }
                        if (!key.equals("") && !val.equals("")) {
                            String add[] = {key, val};
                            count++;
                            Log.d("Inserting", key + ":" + val);
                            cursor.addRow(add);
                        }
                    }
                    Log.d("Count", String.valueOf(count));
                }
                Log.d("Adding local", Arrays.toString(list_of_files));
                for (String file : list_of_files) {
                    query_file(file, cursor);
                }
                return cursor;
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else if (selection.contains("*") && selection.length() == 1) {
            try {
                String forward_query = "query" + ":" + s_port + ":" + selection;
                Log.d("Forward Query", forward_query);
                String response = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, forward_query).get();
                Log.d("Response from *", response);
                String key_value[] = response.split(",");
                Log.d("Keyvalue", Arrays.toString(key_value));
                int count = 0;
                for (String values : key_value) {
                    Log.d("Inserting", values.trim());
                    values = values.trim();
                    String key = "";
                    String val = "";
                    try {
                        key = values.split("=")[0];
                        val = values.split("=")[1];
                    } catch (ArrayIndexOutOfBoundsException arr) {
                        Log.d("Error in arr", arr.toString());
                    }
                    if (!key.equals("") && !val.equals("")) {
                        if (key.contains("{")) {
                            key = key.substring(1);
                        }
                        if (val.contains("}"))
                            val = val.substring(0, 32);
                        String add[] = {key, val};
                        count++;
                        cursor.addRow(add);
                    }
                }
                Log.d("Count", String.valueOf(count));
                return cursor;
            } catch (Exception e) {
                e.printStackTrace();
            }

        } else {
            if (selection.length() > 6) {
                if (Arrays.asList(list_of_files).contains(selection)) {
                    try {
                        query_file(selection, cursor);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    return cursor;
                } else {
                    try {
                        Socket s = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), s_port * 2);
                        DataOutputStream dataOutputStream = new DataOutputStream(s.getOutputStream());
                        DataInputStream dataInputStream = new DataInputStream(s.getInputStream());
                        String message_to_forward = "query" + ":" + s_port + ":" + selection;
                        dataOutputStream.writeUTF(message_to_forward);
                        message = dataInputStream.readUTF();
                        Log.d("Message read", message);
                        String value = message.substring(34, 66);
                        cursor.addRow(new String[]{selection, value});
                        return cursor;
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        return null;
    }

    private void query_file(String selection, MatrixCursor cursor) throws IOException {
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(getContext().openFileInput(selection)));
        String input[] = {selection, bufferedReader.readLine()};
        cursor.addRow(input);
        bufferedReader.close();
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket socket = sockets[0];
            try {
                while (true) {
                    Socket sock = socket.accept();
                    Log.d("Current Port", current_port);
                    Log.d("Predecessor of this", String.valueOf(p_port));
                    Log.d("Successor of this", String.valueOf(s_port));
                    DataInputStream input = new DataInputStream(sock.getInputStream());
                    DataOutputStream output = new DataOutputStream(sock.getOutputStream());
                    String string_input = input.readUTF();
                    Log.d("Server Input", string_input);
                    String msg[] = string_input.split(":");
                    current_node_hash = genHash(current_port);
                    if (msg[0].equals("join")) {
                        String new_join = initialJoin(msg);
                        output.writeUTF("DONE");
                        publishProgress(new_join);
                    } else if (string_input.split(":")[0].equals("new_join")) {

                        try {
                            s_hash = genHash(string_input.split(":")[3]);
                            p_hash = genHash(string_input.split(":")[2]);
                            s_port = Integer.valueOf(node_hash_value.get(s_hash));
                            p_port = Integer.valueOf(node_hash_value.get(p_hash));

                            Log.d("Le Predecessor Port", String.valueOf(p_port));
                            Log.d("Le Successor Port", String.valueOf(s_port));
                            Log.d("Le Predecessor Hash", p_hash);
                            Log.d("Le Successor Hash", s_hash);
                            output.writeUTF("DONE");
                        } catch (NoSuchAlgorithmException nsae) {
                            nsae.printStackTrace();
                        }
                    } else if (string_input.split(":")[0].equals("pre_node")) {
                        p_port = Integer.valueOf(string_input.split(":")[2]);
                        try {
                            p_hash = genHash(String.valueOf(p_port));
                            Log.d("Le Predecessor Port", String.valueOf(p_port));
                            Log.d("Le Successor Port", String.valueOf(s_port));
                            Log.d("Le Predecessor Hash", p_hash);
                            output.writeUTF("DONE");
                        } catch (NoSuchAlgorithmException nsae) {
                            nsae.printStackTrace();
                        }
                    } else if (string_input.split(":")[0].equals("suc_node")) {
                        s_port = Integer.valueOf(string_input.split(":")[2]);
                        try {
                            s_hash = genHash(String.valueOf(s_port));
                            Log.d("Le Successor Port", String.valueOf(s_port));
                            Log.d("Le Predecessor Port", String.valueOf(p_port));
                            Log.d("Le Successor Hash", s_hash);
                            output.writeUTF("DONE");
                        } catch (NoSuchAlgorithmException nsae) {
                            nsae.printStackTrace();
                        }
                    } else if (string_input.split(":")[0].equals("insert")) {
                        ContentValues cv = new ContentValues();
                        cv.put("key", string_input.split(":")[2]);
                        cv.put("value", string_input.split(":")[3]);
                        insert(providerUri, cv);
                        output.writeUTF("DONE");
                    } else if (string_input.split(":")[0].equals("query")) {
                        LinkedHashMap<String, String> key_value = new LinkedHashMap<String, String>();
                        String tokens[] = string_input.split(":");
                        if (tokens[2].equals("*")) {
                            Cursor cursor = query(providerUri, null, "*" + current_port, null, null);
                            assert cursor != null;
//                            Log.d("Cursor Columns:", Arrays.toString(cursor.getColumnNames()));
                            // https://stackoverflow.com/questions/10723770/whats-the-best-way-to-iterate-an-android-cursor
                            try {
                                for (cursor.moveToFirst(); !cursor.isAfterLast(); cursor.moveToNext()) {
                                    key_value.put(cursor.getString(cursor.getColumnIndex("key")), cursor.getString(cursor.getColumnIndex("value")));
                                    cursor.close();
                                }
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        } else {
                            Cursor cursor = query(providerUri, null, tokens[2], null, null);
                            assert cursor != null;
                            cursor.moveToFirst();
                            Log.d("Cursor Columns:", Arrays.toString(cursor.getColumnNames()));
                            while (!cursor.isAfterLast()) {
                                String inner = cursor.getString(cursor.getColumnIndex("value"));
                                String value = null;
                                String arr[] = inner.split("=");
                                for (String item : arr) {
                                    if (!item.equals("{" + tokens[2])) {
                                        value = item.substring(0, 32);
                                    }
                                }
                                Log.d("Value after headache", value);
                                key_value.put(cursor.getString(cursor.getColumnIndex("key")), value);
                                cursor.moveToNext();
                            }
                            cursor.close();
                        }
                        Log.d("Le Predecessor Port", String.valueOf(p_port));
                        Log.d("Le Successor Port", String.valueOf(s_port));
                        Log.d("Le Key_Value", key_value.toString());
                        output.writeUTF(key_value.toString());
                    } else if (string_input.split(":")[0].equals("delete")) {
                        if (string_input.split(":")[2].equals("*"))
                            delete(providerUri, "*" + string_input.split(":")[1], null);
                        else
                            delete(providerUri, string_input, null);
                        output.writeUTF("DONE");
                    }
                    output.flush();
                    output.close();
                    input.close();
                    sock.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
            return null;
        }

        private String initialJoin(String[] msg) {
            String hash = "";
            try {
                hash = genHash(msg[1]);
                nodes_list.add(hash);
            } catch (NoSuchAlgorithmException nsae) {
                nsae.printStackTrace();
            }
            int number_of_nodes = nodes_list.size();
            Collections.sort(nodes_list);
            int node_index = nodes_list.indexOf(hash);
            String new_join = "new_join" + ":" + msg[1];
            Log.d("Initial Join", new_join);
            Log.d("Node Index", String.valueOf(node_index));
            for (String s : nodes_list) {
                Log.d("Node List", node_hash_value.get(s));
            }
            Log.d("Number of nodes", String.valueOf(number_of_nodes));

            new_join = get_new_join_string(number_of_nodes, node_index, new_join);
            Log.d("Returning New Join", new_join);
            return new_join;
        }

        private String get_new_join_string(int number_of_nodes, int node_index, String new_join) {
            if (node_index > 0 && node_index < number_of_nodes - 1) {
                new_join += ":" + node_hash_value.get(nodes_list.get(node_index - 1)) + ":" + node_hash_value.get(nodes_list.get(node_index + 1));
            } else if (node_index == 0) {
                new_join += ":" + node_hash_value.get(nodes_list.get(nodes_list.size() - 1)) + ":" + node_hash_value.get(nodes_list.get(1));
            } else if (node_index == nodes_list.size() - 1) {
                new_join += ":" + node_hash_value.get(nodes_list.get(node_index - 1)) + ":" + node_hash_value.get(nodes_list.get(0));
            }
            return new_join;
        }

        protected void onProgressUpdate(String... strings) {
            Log.d("Strings", Arrays.toString(strings));
            if (strings[0].contains("new_join")) {
                String tokens[] = strings[0].split(":");
                Log.d("Inside ONPU", Arrays.toString(tokens));
                Log.d("String of zero", strings[0]);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, strings[0]);

                String predecessor_string = "pre_node" + ":" + tokens[3] + ":" + tokens[1];
                String successor_string = "suc_node" + ":" + tokens[2] + ":" + tokens[1];
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, successor_string);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, predecessor_string);
            }
        }
    }

    private class ClientTask extends AsyncTask<String, String, String> {

        @Override
        protected String doInBackground(String... strings) {

            Socket s;
            Log.d("Client Task", strings[0]);
            try {
                if (strings[0].split(":")[0].equals("join")) {
                    s = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt("11108"));
                    DataOutputStream output = new DataOutputStream(s.getOutputStream());
                    output.writeUTF(strings[0]);
                    output.flush();

                    s.setSoTimeout(500);
                    DataInputStream input = new DataInputStream(s.getInputStream());
                    String response = "";
                    try {
                        response = input.readUTF();
                    } catch (IOException e) {
                        response = "DONE";
                    }
                    s.close();
                    if (!response.equals("DONE"))
                        return response;
                } else {
                    String msg_port = strings[0].split(":")[1];
                    s = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(msg_port) * 2);
                    DataOutputStream output = new DataOutputStream(s.getOutputStream());
                    output.writeUTF(strings[0]);
                    output.flush();

                    DataInputStream input = new DataInputStream(s.getInputStream());
                    String response = "";
                    try {
                        response = input.readUTF();
                    } catch (IOException e) {
                        response = "DONE";
                    }
                    s.close();
                    Log.d("Response is: ", response + ":" + current_port);
                    if (!response.equals("DONE"))
                        return response;
                }
            } catch (IOException io) {
                io.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        }
    }
}