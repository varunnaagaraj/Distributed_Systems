package edu.buffalo.cse.cse486586.simpledynamo;

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
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Formatter;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.concurrent.ExecutionException;

public class SimpleDynamoProvider extends ContentProvider {
	static final String REMOTE_PORT_0 = "11108";
	static final String REMOTE_PORT_1 = "11112";
	static final String REMOTE_PORT_2 = "11116";
	static final String REMOTE_PORT_3 = "11120";
	static final String REMOTE_PORT_4 = "11124";

	static final String[] ports =
			new String[]{REMOTE_PORT_0, REMOTE_PORT_1, REMOTE_PORT_2, REMOTE_PORT_3, REMOTE_PORT_4};

	static final String[] node_order = new String[]{
			String.valueOf(Integer.parseInt(REMOTE_PORT_4)/2),
			String.valueOf(Integer.parseInt(REMOTE_PORT_1)/2),
			String.valueOf(Integer.parseInt(REMOTE_PORT_0)/2),
			String.valueOf(Integer.parseInt(REMOTE_PORT_2)/2),
			String.valueOf(Integer.parseInt(REMOTE_PORT_3)/2)};

	final Uri providerUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");

	private Uri buildUri(String content, String s) {
		Uri.Builder uri_builder = new Uri.Builder();
		uri_builder.authority(s);
		uri_builder.scheme(content);
		return uri_builder.build();
	}

	static String current_node_hash, current_port;
	HashMap<String, String> node_hash_value = new HashMap<String, String>();
	String s_hash = null, p_hash = null;
	int s_port = 0, p_port = 0;
	String s1=null, s2= null, p1=null, p2=null;
	static boolean recovering = false;


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
//			Log.d("Forward Delete", forward_delete);
			String response = null;
			try {
				response = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, forward_delete).get();
//				Log.d("Response from * del", response);
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
			String forward_delete_rep = "delete_replica" +":"+ s1+":"+ selection;
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, forward_delete_rep);
			String forward_delete_rep2 = "delete_replica"+":" + s2+":"+selection;
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, forward_delete_rep2);
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
		try {
			String key = values.getAsString("key").trim();
			String val = values.getAsString("value").trim();
			Log.d("Key to Insert", key);
			Log.d("Current Port", current_port);
			Log.d("Current Hash", current_node_hash);
			String node = "";
			try {
				if(check_node(key, "5562") >0 && check_node(key, "5556") <=0)
					node="5556";
				else if(check_node(key, "5556") >0 && check_node(key, "5554") <=0)
					node="5554";
				else if(check_node(key, "5554") >0 && check_node(key, "5558") <=0)
					node="5558";
				else if(check_node(key, "5558") >0 && check_node(key, "5560") <=0)
					node="5560";
				else
					node="5562";
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}
			Log.d("Node is", node);
			try{
			if (node.equals(current_port)){
				Log.d("Local write", "done");
				write_file(key, val);
			} else {
				Log.d("Forward write", "done");
				String forward_to_successor = "insert" + ":" + node + ":" + key + ":" + val;
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, forward_to_successor);
			}
			}catch (Exception e)
			{
				Log.d("",e.toString());
			}
			String sps = successor_mapping(node);
			String forward_to_s1 = "insert_replica" + ":" + sps.split(":")[0] + ":" + key + ":" + val;
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, forward_to_s1);
			String forward_to_s2 = "insert_replica" + ":" + sps.split(":")[1] + ":" + key + ":" + val;
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, forward_to_s2);
			getContext().getContentResolver().notifyChange(providerUri, null);

		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	private void write_file(String key, String val) throws IOException {
		FileOutputStream fileOutputStream = getContext().openFileOutput(key.trim(), Context.MODE_PRIVATE);
		OutputStreamWriter outputStreamWriter = new	OutputStreamWriter(fileOutputStream);
		outputStreamWriter.write(val.trim());
		outputStreamWriter.flush();
		outputStreamWriter.close();
		fileOutputStream.close();
	}

	private int check_node(String key, String s) throws NoSuchAlgorithmException {
		return genHash(key.trim()).compareTo(genHash(s.trim()));
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
//		Log.d("Current port OnCreate", current_port);
		try {
			current_node_hash = genHash(current_port);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}

		int node_index = Arrays.asList(node_order).indexOf(portStr);
		p1 = node_order[(((node_index-1)%5) +5 ) %5];
		p2 = node_order[(((node_index-2)%5) +5 ) %5];
		s1 = node_order[(node_index+1)%5];
		s2 = node_order[(node_index+2)%5];
		String values = successor_mapping(portStr);
		s1 = values.split(":")[0];
		s2 = values.split(":")[1];
		p_port = Integer.valueOf(p1);
		s_port = Integer.valueOf(s1);
//		Log.d("Current port#", current_port);
		for (String file: getContext().getFilesDir().list()){
			getContext().deleteFile(file);
		}

		for (String port : node_order) {

			if (!port.equals(current_port)) {
				String recover = "recover" + ":" + port + ":" + current_port;
				try {
					String ack = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, recover).get();
				} catch (InterruptedException e) {
					e.printStackTrace();
				} catch (ExecutionException e) {
					e.printStackTrace();
				}
			}
		}
		return false;
	}

	private String successor_mapping(String node) {
		String port1=null, port2=null;
		if (node.equals("5562"))
		{port1="5556";port2="5554";}
		else if (node.equals("5556"))
		{port1="5554";port2="5558";}
		else if (node.equals("5554"))
		{port1="5558";port2="5560";}
		else if (node.equals("5558"))
		{port1="5560";port2="5562";}
		else if (node.equals("5560"))
		{port1="5562";port2="5556";}
		return port1+":"+port2;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
						String sortOrder) {
		// TODO Auto-generated method stub
//		Log.d("Querying", selection);
//		Log.d("Querying length", String.valueOf(selection.length()));
		String message = "";
		Context context = getContext();
		MatrixCursor cursor = new MatrixCursor(new String[]{"key", "value"});
		String[] list_of_files = context.getFilesDir().list();
		cursor.moveToFirst();
		selection = selection.trim();
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
		} else if(selection.equals("*")){
//			MatrixCursor matrixCursor = new MatrixCursor(new String[]{"key", "value"});
			for (String file: getContext().getFilesDir().list()){
				try {
					query_file(file, cursor);
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			for (String p: node_order){
				if(!p.equals(current_port)){
					try {
//						Socket s = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(p) * 2);
//						DataOutputStream dataOutputStream = new DataOutputStream(s.getOutputStream());
//						DataInputStream dataInputStream = new DataInputStream(s.getInputStream());
						String message_to_forward = "query" + ":" + p + ":" + "@";
//						dataOutputStream.writeUTF(message_to_forward);
						String[] items = {};
						String m = "";
						try {
							m = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message_to_forward).get();
							items = m.split(",");
						} catch (Exception e){
							continue;
						}
//						Log.d("Message read", message);
						for (String i:items){
							i = i.replaceAll("\\{", "");
							String key  = i.split("=")[0];
							String value = i.split("=")[1];
							if(value.endsWith("}"))
								value = value.substring(0, value.length()-1);
//							Log.d("Item", i);
//							Log.d("Item", key);
//							Log.d("Value", value);
							cursor.addRow(new String[]{key.trim(), value.trim()});
						}
//						Log.d("Total count", String.valueOf(cursor.getCount()));
					} catch (Exception io){
						io.printStackTrace();
					}
				}
			}
			return cursor;
		} else if (selection.length() > 6) {
				if(Arrays.asList(getContext().getFilesDir().list()).contains(selection))
					{
						try{
						query_file(selection, cursor);
						} catch(Exception e){
							e.printStackTrace();
						}
					return cursor;
				} else {
					try {
						String node = "";
						if (check_node(selection, "5562") > 0 && check_node(selection, "5556") <= 0)
							node = "5556";
						else if (check_node(selection, "5556") > 0 && check_node(selection, "5554") <= 0)
							node = "5554";
						else if (check_node(selection, "5554") > 0 && check_node(selection, "5558") <= 0)
							node = "5558";
						else if (check_node(selection, "5558") > 0 && check_node(selection, "5560") <= 0)
							node = "5560";
						else
							node = "5562";
						String ports = successor_mapping(node);
						ports = ports+":"+node;
						String[] p = ports.split(":");
						for (String i:p) {
							if(i.equals(current_port))
								continue;
							Socket s = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(i) * 2);
							DataOutputStream dataOutputStream = new DataOutputStream(s.getOutputStream());
							DataInputStream dataInputStream = new DataInputStream(s.getInputStream());
							String message_to_forward = "query" + ":" + i + ":" + selection;
							dataOutputStream.writeUTF(message_to_forward);
							Log.d("Port hit", message_to_forward);
							try {
								message = dataInputStream.readUTF();
							} catch (Exception se){
								se.printStackTrace();
								continue;
							}
							Log.d("Value is", message);
							String value = message.substring(34);
							if (value.endsWith("}")) {
								value = value.substring(0, value.length() - 1);
							}
							cursor.addRow(new String[]{selection.trim(), value.trim()});
							s.close();
							dataInputStream.close();
							dataOutputStream.close();
							break;
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
                    return cursor;
				}

		}
		return null;
	}

	private void query_file(String selection, MatrixCursor cursor) throws IOException {
		BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(getContext().openFileInput(selection)));
		String input[] = {selection.trim(), bufferedReader.readLine().trim()};
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
		protected synchronized Void doInBackground(ServerSocket... sockets) {
			ServerSocket socket = sockets[0];
			try {
				while (true) {
					Socket sock = socket.accept();
					DataInputStream input = new DataInputStream(sock.getInputStream());
					DataOutputStream output = new DataOutputStream(sock.getOutputStream());
					String string_input = input.readUTF();
					Log.d("Server Input", string_input);
					current_node_hash = genHash(current_port);
					if (string_input.split(":")[0].equals("recover")){
//						if (Arrays.asList(getContext().getFilesDir().list()).size() == 0)
//							output.writeUTF("DONE");
//						else {
						recovering = true;
							String origin_port = string_input.split(":")[2];
							LinkedHashMap<String, String> key_value = new LinkedHashMap<String, String>();
							MatrixCursor result = new MatrixCursor(new String[]{"key", "value"});
							for (String file : getContext().getFilesDir().list()) {
								try {
									query_file(file, result);
								} catch (Exception e) {
									e.printStackTrace();
								}
							}
							recovery_initial(key_value, result, origin_port);
							output.writeUTF("DONE");
                        recovering = false;
//						}
					} else if(string_input.split(":")[0].equals("recover_forward")) {
						LinkedHashMap<String, String> recovered_values = new LinkedHashMap<String, String>();
						String received_forward = string_input.split(":")[2];
						if (received_forward.length() > 2) {
							received_forward = received_forward.replaceAll("\\{", "");
							received_forward = received_forward.replaceAll("\\}", "");
							String[] key_value = received_forward.split(",");

							for (String values : key_value) {

								try {
									String key = values.split("=")[0];
									String val = values.split("=")[1];
									Log.d("Key to check", key);
//									Log.d("keyhash", genHash(key));
									recovered_values.put(key.trim(), val.trim());
								} catch (ArrayIndexOutOfBoundsException arr) {
									Log.d("Error in array call", arr.toString());
								}
							}
//							Log.d("Wrote files:", String.valueOf(recovered_values.size()));
							String[] values = recovered_values.toString().split(",");

							for (String item : values) {
								item = item.replaceAll("\\{", "");
								String k = item.split("=")[0];
								String v = item.split("=")[1];
								if (v.endsWith("}")) {
									v = v.substring(0, v.length() - 1);
								}
								Log.d("Writing", k + ":" + v);
								if (!Arrays.asList(getContext().getFilesDir().list()).contains(k)) {
									write_file(k, v);
//									Log.d("Local file list len", String.valueOf(Arrays.asList(getContext().getFilesDir().list()).size()));
								}
							}
						}
						getContext().getContentResolver().notifyChange(providerUri, null);
						output.writeUTF("DONE");
					} else if (string_input.split(":")[0].equals("insert")) {
						ContentValues cv = new ContentValues();
						cv.put("key", string_input.split(":")[2].trim());
						cv.put("value", string_input.split(":")[3].trim());
						insert(providerUri, cv);
						output.writeUTF("DONE");
					} else if (string_input.split(":")[0].equals("insert_replica")) {
						String key = string_input.split(":")[2].trim();
						String value = string_input.split(":")[3].trim();
//						Log.d("Insert replica", key+":"+value);
						write_file(key, value);
						output.writeUTF("DONE");
					} else if (string_input.split(":")[0].equals("query")) {
						LinkedHashMap<String, String> key_value = new LinkedHashMap<String, String>();
						String tokens[] = string_input.split(":");
//						Log.d("Came to query", Arrays.toString(tokens));
						if (tokens[2].equals("*")) {
							Cursor cursor = query(providerUri, null, "*", null, null);
//							Log.d("Returned","true");
							assert cursor != null;
//							Log.d("Got back cursor", String.valueOf(cursor.getCount()));
							// https://stackoverflow.com/questions/10723770/whats-the-best-way-to-iterate-an-android-cursor
							try {
								for (cursor.moveToFirst(); !cursor.isAfterLast(); cursor.moveToNext()) {
									String key = cursor.getString(cursor.getColumnIndex("key"));
									String value = cursor.getString(cursor.getColumnIndex("value"));
//									Log.d("Added KV", key+"="+value);
									key_value.put(key, value);
								}
								cursor.close();
							} catch (Exception e) {
								e.printStackTrace();
							}
						}  else {
							Cursor cursor = query(providerUri, null, tokens[2].trim(), null, null);
							assert cursor != null;
							cursor.moveToFirst();
//							Log.d("Cursor Columns:", Arrays.toString(cursor.getColumnNames()));
							while (!cursor.isAfterLast()) {
								String inner = cursor.getString(cursor.getColumnIndex("value"));
//								Log.d("Inner", inner);
								String value = null;
								String arr[] = inner.split("=");
								for (String item : arr) {
//									Log.d("item value", item);
									if (!item.equals("{" + tokens[2])) {
//										Log.d("item value", "added");
										value = item.substring(0, 32);
									}
								}
//								Log.d("Value after headache", value);
								String k = cursor.getString(cursor.getColumnIndex("key"));
								String v = cursor.getString(cursor.getColumnIndex("value"));
//								Log.d("Added KV @", k+"="+v);
								key_value.put(k, v);
								cursor.moveToNext();
							}
							cursor.close();
						}
//						Log.d("Le Predecessor Port", String.valueOf(p_port));
//						Log.d("Le Successor Port", String.valueOf(s_port));
//						Log.d("Le Key_Value", key_value.toString());
						output.writeUTF(key_value.toString());
					} else if (string_input.split(":")[0].equals("delete")) {
						if (string_input.split(":")[2].equals("*"))
							delete(providerUri, "*" + string_input.split(":")[1], null);
						else
							delete(providerUri, string_input, null);
						output.writeUTF("DONE");
					} else if (string_input.split(":")[0].equals("delete_replica")) {
						for (String file : getContext().getFilesDir().list()) {
							if (string_input.split(":")[2].equals(file)) {
								getContext().deleteFile(file);
							}
						}
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

		private void recovery_initial(LinkedHashMap<String, String> key_value, MatrixCursor result, String origin_port) throws NoSuchAlgorithmException {
			if (result.getCount() >0){
				int index = Arrays.asList(node_order).indexOf(origin_port);
				String pre1 = node_order[(((index-1)%5) +5 ) %5];
				String pre2 = node_order[(((index-2)%5) +5 ) %5];
				result.moveToFirst();
				for (result.moveToFirst(); !result.isAfterLast(); result.moveToNext()) {
					String k = result.getString(result.getColumnIndex("key"));
					String v = result.getString(result.getColumnIndex("value"));
					String node = "";
					if (check_node(k, "5562") > 0 && check_node(k, "5556") <= 0)
						node = "5556";
					else if (check_node(k, "5556") > 0 && check_node(k, "5554") <= 0)
						node = "5554";
					else if (check_node(k, "5554") > 0 && check_node(k, "5558") <= 0)
						node = "5558";
					else if (check_node(k, "5558") > 0 && check_node(k, "5560") <= 0)
						node = "5560";
					else
						node = "5562";
					String[] ports = {origin_port, pre1, pre2};
					if (Arrays.asList(ports).contains(node))
						key_value.put(k.trim(), v.trim());
				}
				Log.d("keyValue added", String.valueOf(result.getCount()));
				result.close();
				String forward_recover = "recover_forward"+":"+ origin_port +":"+ key_value.toString();
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, forward_recover);

			}
		}
	}

	private class ClientTask extends AsyncTask<String, String, String> {

		@Override
		protected synchronized String doInBackground(String... strings) {

			Socket s;
			Log.d("Client Task", strings[0]);
			try {
			    while(recovering) {}
				String msg_port = strings[0].split(":")[1];
//				while (strings[0].contains("query") && Arrays.asList(getContext().getFilesDir().list()).size() ==0){}
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
				if (!response.contains("DONE")) {
					Log.d("Result is: ", response);
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