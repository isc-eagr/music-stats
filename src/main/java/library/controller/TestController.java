package library.controller;

import java.awt.Dimension;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

import javax.imageio.ImageIO;

import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.rendering.ImageType;
import org.apache.pdfbox.rendering.PDFRenderer;
import org.apache.poi.xwpf.usermodel.BreakType;
import org.apache.poi.xwpf.usermodel.XWPFDocument;
import org.apache.poi.xwpf.usermodel.XWPFParagraph;
import org.apache.poi.xwpf.usermodel.XWPFRun;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import com.itextpdf.text.Document;
import com.itextpdf.text.DocumentException;
import com.itextpdf.text.Paragraph;
import com.itextpdf.text.pdf.PdfReader;
import com.itextpdf.text.pdf.PdfWriter;
import com.itextpdf.text.pdf.parser.PdfReaderContentParser;
import com.itextpdf.text.pdf.parser.SimpleTextExtractionStrategy;
import com.itextpdf.text.pdf.parser.TextExtractionStrategy;

import library.entity.Coche;
import library.entity.Persona;
import library.repository.TestRepository;

@Controller
public class TestController {
	
	@Autowired
	private TestRepository testRepository;
	
	@GetMapping("/probandoInsertFK")
	@ResponseBody
	public String probandoInsertFK() {
		Persona p = new Persona();
		p.setNombre("Emmanuel Gomez");
		
		Coche coche1 = new Coche();
		coche1.setNombre("Porsche");
		coche1.setPersona(p);
		
		Coche coche2 = new Coche();
		coche2.setNombre("Mercedes Benz");
		coche2.setPersona(p);
		
		Coche coche3 = new Coche();
		coche3.setNombre("Buggati");
		coche3.setPersona(p);
		
		p.setCoches(List.of(coche1, coche2, coche3));
		
		testRepository.save(p);
		
		return "Éxito";
		
	}
	
	public static void main(String... args) throws DocumentException, FileNotFoundException, IOException {
		
		InputStream docxInputStream = new FileInputStream("C:\\Users\\ing_e\\Desktop\\Acuerdos.docx");
		try (XWPFDocument document = new XWPFDocument(docxInputStream); 
			ByteArrayOutputStream pdfOutputStream = new ByteArrayOutputStream();) {
		    Document pdfDocument = new Document();
		    PdfWriter.getInstance(pdfDocument, pdfOutputStream);
		    pdfDocument.open();
		            
		    List<XWPFParagraph> paragraphs = document.getParagraphs();
		    for (XWPFParagraph paragraph : paragraphs) {
		        pdfDocument.add(new Paragraph(paragraph.getText()));
		    }
		    
		    document.close();
			pdfDocument.close();
		    
		    InputStream in = new ByteArrayInputStream(pdfOutputStream.toByteArray());
		    
		    PDDocument tmpDocument = PDDocument.load(in);
			PDFRenderer pdfRenderer = new PDFRenderer(tmpDocument);
			BufferedImage bim = pdfRenderer.renderImageWithDPI(0, 40, ImageType.RGB);
			
			
			File outputfile = new File("C:\\Users\\ing_e\\Desktop\\image.jpg");
			ImageIO.write(bim, "jpg", outputfile);
		}
	}

}
