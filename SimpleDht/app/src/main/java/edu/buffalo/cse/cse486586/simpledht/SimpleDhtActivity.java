package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentValues;
import android.database.Cursor;
import android.net.Uri;
import android.os.Bundle;
import android.app.Activity;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.widget.TextView;

public class SimpleDhtActivity extends Activity {
    TextView tv;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_simple_dht_main);

        final Uri.Builder uri_builder = new Uri.Builder();
        uri_builder.authority("edu.buffalo.cse.cse486586.simpledht.provider");
        uri_builder.scheme("content");
        final Uri uri = uri_builder.build();
        tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());
        findViewById(R.id.button3).setOnClickListener(
                new OnTestClickListener(tv, getContentResolver()));
        findViewById(R.id.button2).setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View view) {
                Cursor resultCursor = getContentResolver().query(uri, null,
                        "*", null, null);
//                displayCursorOnTextView(resultCursor);

                while (resultCursor.getCount() > 0 && !resultCursor.isAfterLast()){
                    tv.append(resultCursor.getString(resultCursor.getColumnIndex("key"))+":"+resultCursor.getString(resultCursor.getColumnIndex("value")));
                    resultCursor.moveToNext();
                }

            }
        });


//        findViewById(R.id.button3).setOnClickListener(
//                new OnTestClickListener(tv, getContentResolver()));
    }

//    public void displayCursorOnTextView(Cursor cursor) {
//        Log.d("Cursor Size:", Integer.toString(cursor.getCount()));
//        if (cursor.moveToFirst()) {
//            while (!cursor.isAfterLast()) {
//                tv.append(cursor.getString(0) + ":" + cursor.getString(1) + "\n");
//                cursor.moveToNext();
//            }
//        } else {
//            tv.append("Empty Result returned!\n");
//        }
//        cursor.close();
//    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_simple_dht_main, menu);
        return true;
    }

}
