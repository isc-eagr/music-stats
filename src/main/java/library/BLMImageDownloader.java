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
	
	static int startInclusive = 1694;
	static int endInclusive = 1694;
	
	public static void main(String... args) throws IOException{
		
		String userpass = "vatito22:Tr4ilera";
		String basicAuth = "Basic " + new String(Base64.getEncoder().encode(userpass.getBytes()));
		Document document;
		
		for(int i=startInclusive ; i<= endInclusive; i++) {
		
			String numberString = String.valueOf(i);
			//numberString = numberString.length()==2?"0"+numberString:numberString;
			//numberString = numberString.length()==1?"00"+numberString:numberString;
			
		String url = "https://bilatinmen.com/members/latin_men_pictures/pictures_09/model"+numberString+"/";
		
		String[] urlSegments = url.split("/");
		String dirName = urlSegments[urlSegments.length-1];
		try {
			document = Jsoup.connect(url).header("Authorization", basicAuth).get();
		}
		catch(Exception e) {
			document = null;
		}
		
		
		if(document!=null) {
			Element table = document.select("table").first();
			try {
				Files.createDirectory(Path.of("D:/Content/Latino/Bilatinmen/Pics/"+dirName));
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
									
								    Files.copy(in, Paths.get("D:/Content/Latino/Bilatinmen/Pics/"+dirName+"/"+fileName));
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
	}
	
	public static void main1(String... args) throws IOException{
		
		String userpass = "vatito22:Tr4ilera";
		String basicAuth = "Basic " + new String(Base64.getEncoder().encode(userpass.getBytes()));
		
		String baseUrl = "https://bilatinmen.com/members/latin_men_pictures/pictures_09/model";
		char lastLetter = 'y';
		int lastZ = 0;
		
		String[] baseUrlSegments = baseUrl.split("/");
		String dirName = baseUrlSegments[baseUrlSegments.length-1];
		
		try {
			Files.createDirectory(Path.of("C:/Backup/Main/Latino/Bilatinmen/Pics/"+dirName));
		}
		catch(FileAlreadyExistsException faee) {}
		
		char c = 'a';
		int i = 0;
		while(c<=lastLetter || i<= lastZ) {
			String fileName = "extra84";
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
				
		    Files.copy(in, Paths.get("C:/Backup/Main/Latino/Bilatinmen/Pics/"+dirName+"/"+fileName));
		    
		    in.close();
			
		}
	}
}
