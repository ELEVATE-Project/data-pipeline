//package org.shikshalokam.job.mitra.stream.processor.utils
//
//import com.google.cloud.storage.{BlobInfo, Storage, StorageOptions, BlobId}
//import com.google.auth.oauth2.ServiceAccountCredentials
//import org.shikshalokam.job.mitra.stream.processor.task.MitraStreamConfig
//import java.io.FileInputStream
//
///**
// * Generic Cloud Storage Uploader supporting AWS S3, GCP, and Oracle Cloud
// */
//class CloudStorageUploader(config: MitraStreamConfig) {
//
//  private val storageProvider: String = config.cloudStorageProvider
//  //  private var s3Client: S3Client = _
//  private var gcpStorage: Storage = _
//
//  // Initialize the appropriate client based on provider
//  storageProvider.toLowerCase match {
//    case "s3" | "aws" =>
//      //      s3Client = createS3Client()
//      println(s"[CloudStorage] AWS S3 client initialized for bucket: ${config.cloudBucketName}")
//
//    case "gcp" | "google" =>
//      gcpStorage = createGCPStorage()
//      println(s"[CloudStorage] GCP Storage client initialized for bucket: ${config.cloudBucketName}")
//
//    case "oracle" | "oci" =>
//      // Oracle Cloud Storage initialization (placeholder for future implementation)
//      println(s"[CloudStorage] Oracle Cloud Storage not yet implemented")
//      throw new UnsupportedOperationException("Oracle Cloud Storage not yet implemented")
//
//    case _ =>
//      throw new IllegalArgumentException(s"Unsupported cloud storage provider: $storageProvider")
//  }
//
//  /**
//   * Create AWS S3 client
//   */
//  //  private def createS3Client(): S3Client = {
//  //    val credentials = AwsBasicCredentials.create(config.awsAccessKey, config.awsSecretKey)
//  //    S3Client.builder()
//  //      .region(Region.of(config.cloudRegion))
//  //      .credentialsProvider(StaticCredentialsProvider.create(credentials))
//  //      .build()
//  //  }
//
//  /**
//   * Create GCP Storage client
//   */
//  private def createGCPStorage(): Storage = {
//    try {
//      val credentials = if (config.gcpCredentialsPath != null && config.gcpCredentialsPath.nonEmpty) {
//        // Load credentials from JSON file
//        ServiceAccountCredentials.fromStream(new FileInputStream(config.gcpCredentialsPath))
//      } else if (config.gcpProjectId != null && config.gcpProjectId.nonEmpty) {
//        // Use application default credentials
//        null // StorageOptions will use default credentials
//      } else {
//        throw new IllegalArgumentException("GCP credentials path or project ID must be provided")
//      }
//
//      val storageBuilder = StorageOptions.newBuilder()
//
//      if (config.gcpProjectId != null && config.gcpProjectId.nonEmpty) {
//        storageBuilder.setProjectId(config.gcpProjectId)
//      }
//
//      if (credentials != null) {
//        storageBuilder.setCredentials(credentials)
//      }
//
//      storageBuilder.build().getService
//    } catch {
//      case e: Exception =>
//        throw new RuntimeException(s"Failed to initialize GCP Storage client: ${e.getMessage}", e)
//    }
//  }
//
//  /**
//   * Upload file to cloud storage (delegates to appropriate provider)
//   *
//   * @param objectKey Key (path) in cloud storage
//   * @param fileBytes File content as byte array
//   * @return Public URL of uploaded file
//   */
//  def upload(objectKey: String, fileBytes: Array[Byte]): String = {
//    storageProvider.toLowerCase match {
//      //      case "s3" | "aws" => uploadToS3(objectKey, fileBytes, "image/jpeg")
//      case "gcp" | "google" => uploadToGCP(objectKey, fileBytes, "image/jpeg")
//      case "oracle" | "oci" => uploadToOracle(objectKey, fileBytes, "image/jpeg")
//      case _ => throw new IllegalArgumentException(s"Unsupported storage provider: $storageProvider")
//    }
//  }
//
//  /**
//   * Upload file with custom content type
//   *
//   * @param objectKey   Key (path) in cloud storage
//   * @param fileBytes   File content as byte array
//   * @param contentType MIME type of the file
//   * @return Public URL of uploaded file
//   */
//  def uploadWithContentType(objectKey: String, fileBytes: Array[Byte], contentType: String): String = {
//    storageProvider.toLowerCase match {
//      //      case "s3" | "aws" => uploadToS3(objectKey, fileBytes, contentType)
//      case "gcp" | "google" => uploadToGCP(objectKey, fileBytes, contentType)
//      case "oracle" | "oci" => uploadToOracle(objectKey, fileBytes, contentType)
//      case _ => throw new IllegalArgumentException(s"Unsupported storage provider: $storageProvider")
//    }
//  }
//
//  /**
//   * Upload to AWS S3
//   */
//  //  private def uploadToS3(objectKey: String, fileBytes: Array[Byte], contentType: String): String = {
//  //    try {
//  //      println(s"[S3] Uploading file: $objectKey (${fileBytes.length} bytes)")
//  //
//  //      val putObjectRequest = PutObjectRequest.builder()
//  //        .bucket(config.cloudBucketName)
//  //        .key(objectKey)
//  //        .contentType(contentType)
//  //        .contentLength(fileBytes.length.toLong)
//  //        .acl(ObjectCannedACL.PUBLIC_READ) // Make publicly readable
//  //        .build()
//  //
//  //      val requestBody = RequestBody.fromBytes(fileBytes)
//  //      val response = s3Client.putObject(putObjectRequest, requestBody)
//  //
//  //      // Construct public URL
//  //      val publicUrl = s"https://${config.cloudBucketName}.s3.${config.cloudRegion}.amazonaws.com/$objectKey"
//  //
//  //      println(s"[S3] Upload successful. URL: $publicUrl")
//  //      println(s"[S3] ETag: ${response.eTag()}")
//  //
//  //      publicUrl
//  //    } catch {
//  //      case e: S3Exception =>
//  //        throw new RuntimeException(s"S3 upload failed - Status: ${e.statusCode()}, Error: ${e.awsErrorDetails().errorMessage()}", e)
//  //      case e: Exception =>
//  //        throw new RuntimeException(s"Failed to upload to S3: ${e.getMessage}", e)
//  //    }
//  //  }
//
//  /**
//   * Upload to Google Cloud Storage
//   */
//  private def uploadToGCP(objectKey: String, fileBytes: Array[Byte], contentType: String): String = {
//    try {
//      println(s"[GCP] Uploading file: $objectKey (${fileBytes.length} bytes)")
//
//      // Create blob info
//      val blobId = BlobId.of(config.cloudBucketName, objectKey)
//      val blobInfo = BlobInfo.newBuilder(blobId)
//        .setContentType(contentType)
//        .build()
//
//      // Upload the file
//      val blob = gcpStorage.create(blobInfo, fileBytes)
//
//      // Make the blob publicly readable (optional)
//      if (config.makePublic) {
//        import com.google.cloud.storage.Acl
//        import com.google.cloud.storage.Acl.{Role, User}
//        val acl = Acl.of(User.ofAllUsers(), Role.READER)
//        gcpStorage.createAcl(blobId, acl)
//      }
//
//      // Construct public URL
//      val publicUrl = if (config.makePublic) {
//        s"https://storage.googleapis.com/${config.cloudBucketName}/$objectKey"
//      } else {
//        // Return authenticated URL
//        blob.getMediaLink
//      }
//
//      println(s"[GCP] Upload successful. URL: $publicUrl")
//      println(s"[GCP] Generation: ${blob.getGeneration}")
//
//      publicUrl
//    } catch {
//      case e: com.google.cloud.storage.StorageException =>
//        throw new RuntimeException(s"GCP upload failed - Code: ${e.getCode}, Reason: ${e.getReason}, Message: ${e.getMessage}", e)
//      case e: Exception =>
//        throw new RuntimeException(s"Failed to upload to GCP: ${e.getMessage}", e)
//    }
//  }
//
//  /**
//   * Upload to Oracle Cloud Storage (placeholder for future implementation)
//   */
//  private def uploadToOracle(objectKey: String, fileBytes: Array[Byte], contentType: String): String = {
//    // Oracle Cloud Storage implementation would go here
//    // This would use the Oracle Cloud Infrastructure SDK
//    throw new UnsupportedOperationException("Oracle Cloud Storage upload not yet implemented")
//  }
//
//  //  /**
//  //   * Test connection to cloud storage
//  //   */
//  //  def testConnection(): Boolean = {
//  //    try {
//  //      storageProvider.toLowerCase match {
//  //        case "s3" | "aws" =>
//  //          s3Client.headBucket(builder => builder.bucket(config.cloudBucketName))
//  //          println(s"[S3] Connection test successful. Bucket ${config.cloudBucketName} is accessible.")
//  //          true
//  //
//  //        case "gcp" | "google" =>
//  //          val bucket = gcpStorage.get(config.cloudBucketName)
//  //          if (bucket != null && bucket.exists()) {
//  //            println(s"[GCP] Connection test successful. Bucket ${config.cloudBucketName} is accessible.")
//  //            true
//  //          } else {
//  //            println(s"[GCP] Bucket ${config.cloudBucketName} does not exist or is not accessible.")
//  //            false
//  //          }
//  //
//  //        case _ =>
//  //          println(s"[CloudStorage] Connection test not implemented for provider: $storageProvider")
//  //          false
//  //      }
//  //    } catch {
//  //      case e: S3Exception =>
//  //        println(s"[S3] Connection test failed - Status: ${e.statusCode()}, Error: ${e.awsErrorDetails().errorMessage()}")
//  //        false
//  //      case e: com.google.cloud.storage.StorageException =>
//  //        println(s"[GCP] Connection test failed - Code: ${e.getCode}, Reason: ${e.getReason}")
//  //        false
//  //      case e: Exception =>
//  //        println(s"[CloudStorage] Connection test failed: ${e.getMessage}")
//  //        false
//  //    }
//  //  }
//
//  /**
//   * Close cloud storage clients
//   */
//  def close(): Unit = {
//    try {
//      //      if (s3Client != null) {
//      //        s3Client.close()
//      //        println(s"[S3] Client closed successfully")
//      //      }
//
//      // GCP Storage client doesn't need explicit closing
//      if (gcpStorage != null) {
//        println(s"[GCP] Storage client released")
//      }
//    } catch {
//      case e: Exception =>
//        println(s"[CloudStorage] Error closing client: ${e.getMessage}")
//    }
//  }
//}