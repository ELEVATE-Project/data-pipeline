//package org.shikshalokam.job.mitra.stream.processor.utils
//
//import java.io.{File, FileOutputStream, InputStream}
//import java.net.URL
//import java.nio.file.{Files, Path, Paths}
//import scala.sys.process._
//import scala.util.{Try, Success, Failure}
//
///**
// * Image Processor - Downloads images and applies face blurring using deface command
// */
//class ImageProcessor {
//
//  private val tempDir: Path = Paths.get(System.getProperty("java.io.tmpdir"), "story_images")
//
//  // Create temp directory on initialization
//  Try {
//    Files.createDirectories(tempDir)
//    println(s"[ImageProcessor] Temp directory created: $tempDir")
//  }
//
//  /**
//   * Download image from URL and return as byte array
//   * @param imageUrl URL of the image
//   * @return Image bytes
//   */
//  def downloadImage(imageUrl: String): Array[Byte] = {
//    var connection: java.net.URLConnection = null
//    var inputStream: InputStream = null
//
//    try {
//      println(s"[Image] Downloading from: $imageUrl")
//
//      connection = new URL(imageUrl).openConnection()
//      connection.setConnectTimeout(30000) // 30 seconds
//      connection.setReadTimeout(30000)
//      connection.setRequestProperty("User-Agent", "Mozilla/5.0")
//      inputStream = connection.getInputStream
//
//      // Read all bytes
//      val bytes = Stream.continually(inputStream.read).takeWhile(_ != -1).map(_.toByte).toArray
//
//      println(s"[Image] Downloaded successfully. Size: ${bytes.length} bytes")
//
//      bytes
//    } catch {
//      case e: java.net.SocketTimeoutException =>
//        throw new RuntimeException(s"Timeout while downloading image from $imageUrl", e)
//      case e: java.net.UnknownHostException =>
//        throw new RuntimeException(s"Unknown host for image URL: $imageUrl", e)
//      case e: java.io.IOException =>
//        throw new RuntimeException(s"IO error while downloading image: ${e.getMessage}", e)
//      case e: Exception =>
//        throw new RuntimeException(s"Failed to download image from $imageUrl: ${e.getMessage}", e)
//    } finally {
//      if (inputStream != null) {
//        try { inputStream.close() } catch { case _: Exception => }
//      }
//    }
//  }
//
//  /**
//   * Blur faces in image using deface command-line tool
//   * @param imageBytes Original image bytes
//   * @param imageIndex Index for temp file naming
//   * @return Blurred image bytes
//   */
//  def blurFaces(imageBytes: Array[Byte], imageIndex: Int): Array[Byte] = {
//    val timestamp = System.currentTimeMillis()
//    val inputFile = tempDir.resolve(s"input_${imageIndex}_${timestamp}.jpg").toFile
//    val outputFile = tempDir.resolve(s"blurred_${imageIndex}_${timestamp}.jpg").toFile
//
//    try {
//      println(s"[Image] Blurring faces for image $imageIndex...")
//
//      // Write input image to temp file
//      val fos = new FileOutputStream(inputFile)
//      try {
//        fos.write(imageBytes)
//        fos.flush()
//      } finally {
//        fos.close()
//      }
//
//      println(s"[Image] Input file written: ${inputFile.getAbsolutePath}")
//
//      // Execute deface command
//      // Command: deface input_file --output output_file
//      val command = Seq("deface", inputFile.getAbsolutePath, "--output", outputFile.getAbsolutePath)
//
//      println(s"[Image] Executing command: ${command.mkString(" ")}")
//
//      val result = Try {
//        // Capture stdout and stderr
//        val logger = ProcessLogger(
//          (out: String) => println(s"[deface stdout] $out"),
//          (err: String) => println(s"[deface stderr] $err")
//        )
//
//        val exitCode = command.!(logger)
//        if (exitCode != 0) {
//          throw new RuntimeException(s"deface command failed with exit code: $exitCode")
//        }
//      }
//
//      result match {
//        case Success(_) =>
//          // Read blurred image
//          if (!outputFile.exists()) {
//            throw new RuntimeException("deface did not create output file")
//          }
//
//          val blurredBytes = Files.readAllBytes(outputFile.toPath)
//          println(s"[Image] Face blurring completed. Output size: ${blurredBytes.length} bytes")
//
//          blurredBytes
//
//        case Failure(e) =>
//          throw new RuntimeException(s"Failed to execute deface command: ${e.getMessage}", e)
//      }
//    } catch {
//      case e: java.io.IOException =>
//        throw new RuntimeException(s"IO error during face blurring: ${e.getMessage}", e)
//      case e: Exception =>
//        throw new RuntimeException(s"Face blurring failed: ${e.getMessage}", e)
//    } finally {
//      // Clean up temp files
//      try {
//        if (inputFile.exists()) {
//          inputFile.delete()
//          println(s"[Image] Cleaned up input file: ${inputFile.getName}")
//        }
//        if (outputFile.exists()) {
//          outputFile.delete()
//          println(s"[Image] Cleaned up output file: ${outputFile.getName}")
//        }
//      } catch {
//        case e: Exception =>
//          println(s"[Image] Warning: Failed to cleanup temp files: ${e.getMessage}")
//      }
//    }
//  }
//
//  /**
//   * Clean up temp directory (call this during shutdown if needed)
//   */
//  def cleanup(): Unit = {
//    try {
//      if (Files.exists(tempDir)) {
//        println(s"[ImageProcessor] Cleaning up temp directory: $tempDir")
//        Files.walk(tempDir)
//          .sorted(java.util.Comparator.reverseOrder())
//          .forEach(Files.delete)
//        println(s"[ImageProcessor] Temp directory cleaned up successfully")
//      }
//    } catch {
//      case e: Exception =>
//        println(s"[ImageProcessor] Failed to cleanup temp directory: ${e.getMessage}")
//    }
//  }
//}