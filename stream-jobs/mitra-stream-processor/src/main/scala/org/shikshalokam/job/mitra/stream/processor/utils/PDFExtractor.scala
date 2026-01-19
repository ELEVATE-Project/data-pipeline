package org.shikshalokam.job.mitra.stream.processor.utils

import org.apache.pdfbox.pdmodel.PDDocument
import org.apache.pdfbox.text.PDFTextStripper
import com.optimaize.langdetect.LanguageDetectorBuilder
import com.optimaize.langdetect.ngram.NgramExtractors
import com.optimaize.langdetect.profiles.LanguageProfileReader

import java.io.InputStream
import java.net.{HttpURLConnection, URL}
import scala.util.Try

/**
 * PDF text extraction utility - equivalent to Python's process_pdf_and_extract_text
 */
class PDFExtractor {

  // Initialize language detector (like langdetect in Python)
  private val languageDetector = {
    val languageProfiles = new LanguageProfileReader().readAllBuiltIn()
    LanguageDetectorBuilder.create(NgramExtractors.standard())
      .withProfiles(languageProfiles)
      .build()
  }

  /**
   * Download PDF from URL and extract all text
   * Equivalent to Python's: process_pdf_and_extract_text(pdf_url)
   *
   * @param pdfUrl URL to the PDF document
   * @return Either error message or (extracted text, detected language)
   */
  def extractTextFromPdf(pdfUrl: String, logPrefix: String): Either[String, (String, String)] = {
    var connection: HttpURLConnection = null

    try {
      println(s"$logPrefix [PDF] Downloading PDF from: $pdfUrl")

      // 1. Download PDF
      val url = new URL(pdfUrl)
      connection = url.openConnection().asInstanceOf[HttpURLConnection]
      connection.setRequestMethod("GET")
      connection.setConnectTimeout(30000) // 30 seconds
      connection.setReadTimeout(60000) // 60 seconds
      connection.setRequestProperty("User-Agent", "Mozilla/5.0")
      connection.setInstanceFollowRedirects(true)

      val responseCode = connection.getResponseCode

      if (responseCode != 200) {
        return Left(s"$logPrefix [PDF] HTTP error downloading PDF: $responseCode")
      }

      // 2. Extract text using PDFBox (like PyMuPDF/fitz in Python)
      val inputStream: InputStream = connection.getInputStream
      val (text, language) = extractTextAndDetectLanguage(inputStream)

      inputStream.close()

      if (text.trim.isEmpty) {
        Left(s"$logPrefix [PDF] PDF contains no readable text content")
      } else {
        println(s"$logPrefix [PDF] Successfully extracted ${text.length} characters, detected language: $language")
        Right((text, language))
      }

    } catch {
      case e: java.net.SocketTimeoutException =>
        Left(s"$logPrefix [PDF] Timeout downloading PDF: ${e.getMessage}")
      case e: java.net.UnknownHostException =>
        Left(s"$logPrefix [PDF] Unknown host: ${e.getMessage}")
      case e: javax.net.ssl.SSLException =>
        Left(s"$logPrefix [PDF] SSL error: ${e.getMessage}")
      case e: Exception =>
        Left(s"$logPrefix [PDF] PDF processing error: ${e.getMessage}")
    } finally {
      if (connection != null) {
        try {
          connection.disconnect()
        } catch {
          case _: Exception => // Ignore
        }
      }
    }
  }

  /**
   * Extract text from PDF input stream using Apache PDFBox
   * Equivalent to Python's fitz.open() and page.get_text()
   */
  private def extractTextAndDetectLanguage(inputStream: InputStream): (String, String) = {
    var document: PDDocument = null

    try {
      // Load PDF document
      document = PDDocument.load(inputStream)
      val stripper = new PDFTextStripper()
      stripper.setSortByPosition(true)

      // Extract text from all pages (like Python's loop through pages)
      val text = new StringBuilder()
      val pageCount = document.getNumberOfPages()

      for (pageNum <- 0 until pageCount) {
        stripper.setStartPage(pageNum + 1)
        stripper.setEndPage(pageNum + 1)
        text.append(stripper.getText(document))
        text.append("\n\n")
      }

      val extractedText = text.toString.trim

      // Detect language (like langdetect in Python)
      val detectedLanguage = detectLanguage(extractedText)

      (extractedText, detectedLanguage)

    } finally {
      if (document != null) {
        try {
          document.close()
        } catch {
          case _: Exception => // Ignore
        }
      }
    }
  }

  /**
   * Detect language from text
   * Equivalent to Python's: from langdetect import detect
   */
  private def detectLanguage(text: String): String = {
    if (text.trim.isEmpty) return "Unknown"

    Try {
      // Take first 1000 chars for detection (faster)
      val sample = if (text.length > 1000) text.substring(0, 1000) else text
      val langDetectResult = languageDetector.detect(sample)

      if (langDetectResult.isPresent) {
        val langCode = langDetectResult.get().getLanguage
        // Map language codes to full names
        mapLanguageCode(langCode)
      } else {
        "Unknown"
      }
    }.getOrElse {
      // Fallback: detect by character range (for Indian languages)
      detectByCharacterRange(text)
    }
  }

  /**
   * Map ISO language codes to full language names
   */
  private def mapLanguageCode(code: String): String = {
    code.toLowerCase match {
      case "en" => "English"
      case "hi" => "Hindi"
      case "kn" => "Kannada"
      case "ta" => "Tamil"
      case "te" => "Telugu"
      case "bn" => "Bengali"
      case "mr" => "Marathi"
      case "gu" => "Gujarati"
      case "ml" => "Malayalam"
      case "pa" => "Punjabi"
      case _ => code.toUpperCase
    }
  }

  /**
   * Fallback language detection using Unicode character ranges
   */
  private def detectByCharacterRange(text: String): String = {
    val sample = if (text.length > 500) text.substring(0, 500) else text

    if (sample.matches(".*[\\u0900-\\u097F].*")) "Hindi"
    else if (sample.matches(".*[\\u0C80-\\u0CFF].*")) "Kannada"
    else if (sample.matches(".*[\\u0B80-\\u0BFF].*")) "Tamil"
    else if (sample.matches(".*[\\u0C00-\\u0C7F].*")) "Telugu"
    else if (sample.matches(".*[\\u0980-\\u09FF].*")) "Bengali"
    else if (sample.matches(".*[\\u0A80-\\u0AFF].*")) "Gujarati"
    else if (sample.matches(".*[\\u0900-\\u097F].*")) "Marathi"
    else "English"
  }

  /**
   * Truncate text if it exceeds max length (to manage token limits)
   */
  def truncateIfNeeded(text: String, maxChars: Int = 40000, logPrefix: String): String = {
    if (text.length <= maxChars) {
      text
    } else {
      println(s"$logPrefix [PDF] Truncating text from ${text.length} to $maxChars characters")

      // Try to truncate at sentence boundary
      val truncated = text.substring(0, maxChars)
      val lastPeriod = truncated.lastIndexOf('.')
      val lastQuestion = truncated.lastIndexOf('?')
      val lastExclamation = truncated.lastIndexOf('!')

      val lastSentenceEnd = Math.max(Math.max(lastPeriod, lastQuestion), lastExclamation)

      if (lastSentenceEnd > maxChars * 0.7) {
        truncated.substring(0, lastSentenceEnd + 1) + "\n\n[Content truncated for length...]"
      } else {
        truncated + "\n\n[Content truncated for length...]"
      }
    }
  }
}