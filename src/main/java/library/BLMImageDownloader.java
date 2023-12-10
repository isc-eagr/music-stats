package library;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Base64;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class BLMImageDownloader {
	
	public static void main(String... args) throws IOException{
		
		String userpass = "vatito2:Tr4ilera";
		String basicAuth = "Basic " + new String(Base64.getEncoder().encode(userpass.getBytes()));
		
		for(int i=1614 ; i<= 1615; i++) {
		
		String url = "http://bilatinmen.com/members/latin_men_pictures/pictures_09/model"+String.valueOf(i)+"/";
		
		String[] urlSegments = url.split("/");
		String dirName = urlSegments[urlSegments.length-1];
		Document document = Jsoup.connect(url).header("Authorization", basicAuth).get();
		
		Element table = document.select("table").first();
		try {
			Files.createDirectory(Path.of("C:/Users/ing_e/Dropbox/Backup/Latino/Bilatinmen/Pics/"+dirName));
		}
		catch(FileAlreadyExistsException faee) {}
		Elements trs = table.select("tbody").select("tr");
			trs.forEach(tr -> {
				InputStream in = null;
				try{
					Elements tds = tr.select("td");
					if(tds.size()>0) {
						Element td = tds.get(1);
						Elements as = td.select("a");
						
						if(as.size()>0) {
							Element a = as.first();
						
							String fileName = a.text();
							if(fileName.startsWith("model") && fileName.endsWith(".jpg")) {
								URL urlImg = new URL(url+fileName);

								URLConnection uc = urlImg.openConnection();
								uc.setRequestProperty ("Authorization", basicAuth);
								in = uc.getInputStream();
								
							    Files.copy(in, Paths.get("C:/Users/ing_e/Dropbox/Backup/Latino/Bilatinmen/Pics/"+dirName+"/"+fileName));
						    }
						}
					}
				}catch(Exception e) {
					e.printStackTrace();
				}finally{
					if(in != null) {
						try {
							in.close();
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
				}
			});
			
		}
	}
	
	public static void main1(String... args) throws IOException{
		
		String userpass = "vatito2:Tr4ilera";
		String basicAuth = "Basic " + new String(Base64.getEncoder().encode(userpass.getBytes()));
		
		String baseUrl = "http://bilatinmen.com/members/latin_men_pictures/pictures_05/model231/";
		char lastLetter = 'z';
		int lastZ = 9;
		
		String[] baseUrlSegments = baseUrl.split("/");
		String dirName = baseUrlSegments[baseUrlSegments.length-1];
		
		try {
			Files.createDirectory(Path.of("C:/Users/ing_e/Dropbox/Backup/Latino/Bilatinmen/Pics/"+dirName));
		}
		catch(FileAlreadyExistsException faee) {}
		
		char c = 'a';
		int i = 0;
		while(c<=lastLetter || i<= lastZ) {
			String fileName = dirName;
			if(c == 'z' && i>0) {
				fileName += c+""+i+".jpg";
				i++;
				if(i>lastZ) {c++;}
			}else {
				fileName += c+".jpg";
				if(c == 'z' && i==0) {i++;}
				else{c++;}
			}
			
			URL urlImg = new URL(baseUrl+fileName);

			URLConnection uc = urlImg.openConnection();
			uc.setRequestProperty ("Authorization", basicAuth);
			InputStream in = uc.getInputStream();
				
		    Files.copy(in, Paths.get("C:/Users/ing_e/Dropbox/Backup/Latino/Bilatinmen/Pics/"+dirName+"/"+fileName));
		    
		    in.close();
			
		}
	}
}
