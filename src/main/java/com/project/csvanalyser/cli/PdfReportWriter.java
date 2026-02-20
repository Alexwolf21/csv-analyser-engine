package com.project.csvanalyser.cli;

import com.lowagie.text.Document;
import com.lowagie.text.Font;
import com.lowagie.text.Paragraph;
import com.lowagie.text.pdf.PdfWriter;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Writes the human-readable report text to a PDF file.
 */
final class PdfReportWriter {

    private static final float FONT_SIZE = 11f;

    static void write(String reportText, Path outputPath) throws IOException {
        Document doc = new Document();
        try (var fos = Files.newOutputStream(outputPath)) {
            PdfWriter.getInstance(doc, fos);
            doc.open();
            Font font = new Font(Font.HELVETICA, FONT_SIZE, Font.NORMAL);
            Font boldFont = new Font(Font.HELVETICA, FONT_SIZE, Font.BOLD);
            String[] lines = reportText.split("\n");
            for (String line : lines) {
                boolean bold = line.startsWith("GROUP:") || line.startsWith("TOP ") || line.equals("---");
                doc.add(new Paragraph(line, bold ? boldFont : font));
            }
            doc.close();
        }
    }
}
