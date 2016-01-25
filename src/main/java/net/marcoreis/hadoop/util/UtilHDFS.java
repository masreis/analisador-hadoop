package net.marcoreis.hadoop.util;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.text.MessageFormat;
import java.util.Collection;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonReader;

/**
 * * ===== HTTP GET <br/>
 * <li>OPEN (see FileSystem.open)
 * <li>GETFILESTATUS (see FileSystem.getFileStatus)
 * <li>LISTSTATUS (see FileSystem.listStatus)
 * <li>GETCONTENTSUMMARY (see FileSystem.getContentSummary)
 * <li>GETFILECHECKSUM (see FileSystem.getFileChecksum)
 * <li>GETHOMEDIRECTORY (see FileSystem.getHomeDirectory)
 * <li>GETDELEGATIONTOKEN (see FileSystem.getDelegationToken)
 * <li>GETDELEGATIONTOKENS (see FileSystem.getDelegationTokens) <br/>
 * ===== HTTP PUT <br/>
 * <li>CREATE (see FileSystem.create)
 * <li>MKDIRS (see FileSystem.mkdirs)
 * <li>CREATESYMLINK (see FileContext.createSymlink)
 * <li>RENAME (see FileSystem.rename)
 * <li>SETREPLICATION (see FileSystem.setReplication)
 * <li>SETOWNER (see FileSystem.setOwner)
 * <li>SETPERMISSION (see FileSystem.setPermission)
 * <li>SETTIMES (see FileSystem.setTimes)
 * <li>RENEWDELEGATIONTOKEN (see FileSystem.renewDelegationToken)
 * <li>CANCELDELEGATIONTOKEN (see FileSystem.cancelDelegationToken) <br/>
 * ===== HTTP POST <br/>
 * APPEND (see FileSystem.append) <br/>
 * ===== HTTP DELETE <br/>
 * DELETE (see FileSystem.delete)
 **/

public class UtilHDFS {
    public Collection<String> listarArquivos(String diretorio) {
	try {
	    String spec = MessageFormat.format("http://spock:50070/webhdfs/v1{0}?op=LISTSTATUS", diretorio);
	    URL url = new URL(spec);
	    InputStream is = url.openStream();
	    Reader in = new InputStreamReader(is);
	    JsonParser parser = new JsonParser();
	    GsonBuilder b = new GsonBuilder();
	    Gson gson = b.create();
	    Object s = gson.fromJson(in, Object.class);
	    return null;
	} catch (Exception e) {
	    throw new RuntimeException(e);
	}
    }

    public static void main(String[] args) {
	UtilHDFS util = new UtilHDFS();
	util.listarArquivos("/wikipedia/entrada");
    }
}
