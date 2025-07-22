package org.shikshalokam.job.user.stream.processor.domain

import org.shikshalokam.job.domain.reader.JobRequest

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.time.Instant
import scala.collection.immutable.Stream
import scala.language.postfixOps

class Event(eventMap: java.util.Map[String, Any], partition: Int, offset: Long) extends JobRequest(eventMap, partition, offset) {

  def entity: String = readOrDefault[String]("entity", null)

  def eventType: String = readOrDefault[String]("eventType", null)

  def userId: Int = readOrDefault[Int]("entityId", -1) //Can't be updated

  def tenantCode: String = extractValue[String]("tenant_code").getOrElse(null) //Can't be updated

  def username: String = extractValue[String]("username").getOrElse(null)

  def name: String = extractValue[String]("name").getOrElse(null)
  def status: String = extractValue[String]("status").getOrElse(null)
  def isDeleted: Boolean = extractValue[Boolean]("deleted").getOrElse(false)
  def createdBy: Int = extractValue[Int]("created_by").getOrElse(-1)

  def createdAt: Timestamp = {
    extractValue[Any]("created_at") match {
      case Some(ts: Timestamp) => ts
      case Some(s: String) if s.trim.nonEmpty =>
        try {
          Timestamp.valueOf(s)
        } catch {
          case _: IllegalArgumentException =>
            try {
              Timestamp.from(Instant.parse(s))
            } catch {
              case _: Exception =>
                val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
                new Timestamp(formatter.parse(s).getTime)
            }
        }
      case _ => new Timestamp(System.currentTimeMillis())
    }
  }

  def updatedAt: Timestamp = {
    extractValue[Any]("updated_at") match {
      case Some(ts: Timestamp) => ts
      case Some(s: String) if s.trim.nonEmpty =>
        try {
          Timestamp.valueOf(s)
        } catch {
          case _: IllegalArgumentException =>
            try {
              Timestamp.from(Instant.parse(s))
            } catch {
              case _: Exception =>
                val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
                new Timestamp(formatter.parse(s).getTime)
            }
        }
      case _ => new Timestamp(System.currentTimeMillis())
    }
  }

  def userProfileOneId: String = extractValue[String]("state.id").getOrElse(null)
  def userProfileOneName: String = extractValue[String]("state.name").getOrElse(null)
  def userProfileOneExternalId: String = extractValue[String]("state.externalId").getOrElse(null)

  def userProfileTwoId: String = extractValue[String]("district.id").getOrElse(null)
  def userProfileTwoName: String = extractValue[String]("district.name").getOrElse(null)
  def userProfileTwoExternalId: String = extractValue[String]("district.externalId").getOrElse(null)

  def userProfileThreeId: String = extractValue[String]("block.id").getOrElse(null)
  def userProfileThreeName: String = extractValue[String]("block.name").getOrElse(null)
  def userProfileThreeExternalId: String = extractValue[String]("block.externalId").getOrElse(null)

  def userProfileFourId: String = extractValue[String]("cluster.id").getOrElse(null)
  def userProfileFourName: String = extractValue[String]("cluster.name").getOrElse(null)
  def userProfileFourExternalId: String = extractValue[String]("cluster.externalId").getOrElse(null)

  def userProfileFiveId: String = extractValue[String]("school.id").getOrElse(null)
  def userProfileFiveName: String = extractValue[String]("school.name").getOrElse(null)
  def userProfileFiveExternalId: String = extractValue[String]("school.externalId").getOrElse(null)

//  def oldValues: Map[String, Any] = readOrDefault[Map[String, Any]]("oldValues", Map.empty)
//
//  def newValues: Map[String, Any] = readOrDefault[Map[String, Any]]("newValues", Map.empty)


  def extractValue[T](key: String): Option[T] = {
    val direct = Option(readOrDefault[T](key, null.asInstanceOf[T]))

    var fromNew: Option[T] = None
    var fromOld: Option[T] = None

    if (eventType == "update" || eventType == "bulk-update") {
      fromNew = Option(readOrDefault[T](s"newValues.$key", null.asInstanceOf[T]))
      fromOld = Option(readOrDefault[T](s"oldValues.$key", null.asInstanceOf[T]))
    }

    (direct orElse fromNew orElse fromOld).filter(_ != null)
  }

  def organizations: List[Map[String, Any]] = {
    val fromDefault = Option(readOrDefault[List[Map[String, Any]]]("organizations", null)).getOrElse(List.empty)

    var fromNew: List[Map[String, Any]] = List.empty
    var fromOld: List[Map[String, Any]] = List.empty

    if (eventType == "update" || eventType == "bulk-update") {
      fromNew = Option(readOrDefault[List[Map[String, Any]]]("newValues.organizations", null)).getOrElse(List.empty)
      fromOld = Option(readOrDefault[List[Map[String, Any]]]("oldValues.organizations", null)).getOrElse(List.empty)
    }

    (fromDefault ++ fromOld ++ fromNew).distinct
  }
  def organizationsId: String = extractValue[String]("organizations.id").getOrElse("")


  //Nested Extraction
  def extractNestedValue[T](base: String, key: String): Option[T] = {
    val baseMap = Option(readOrDefault[Map[String, Any]](base, null)).getOrElse(Map.empty)

    var fromNew: Map[String, Any] = Map.empty
    var fromOld: Map[String, Any] = Map.empty

    if (eventType == "update" || eventType == "bulk-update") {
      fromNew = Option(readOrDefault[Map[String, Any]](s"newValues.$base", null)).getOrElse(Map.empty)
      fromOld = Option(readOrDefault[Map[String, Any]](s"oldValues.$base", null)).getOrElse(Map.empty)
    }

    val combined = baseMap ++ fromOld ++ fromNew
    Option(combined.getOrElse(key, null).asInstanceOf[T]).filter(_ != null)
  }
  def professionalRoleId: String = extractNestedValue[String]("professional_role", "id").getOrElse(null)
  def professionalRoleName: String = extractNestedValue[String]("professional_role", "name").getOrElse(null)


  def professionalSubroles: List[Map[String, Any]] = {
    val fromDefault = Option(readOrDefault[List[Map[String, Any]]]("professional_subroles", null)).getOrElse(List.empty)

    var fromNew: List[Map[String, Any]] = List.empty
    var fromOld: List[Map[String, Any]] = List.empty

    if (eventType == "update" || eventType == "bulk-update") {
      fromNew = Option(readOrDefault[List[Map[String, Any]]]("newValues.professional_subroles", null)).getOrElse(List.empty)
      fromOld = Option(readOrDefault[List[Map[String, Any]]]("oldValues.professional_subroles", null)).getOrElse(List.empty)
    }

    (fromDefault ++ fromOld ++ fromNew).distinct
  }



  //  println(s"userId: $userId")
//  println(s"tenantCode: $tenantCode")
//  println(s"username: $username")
//
//  println(s"name: $name")
//  println(s"status: $status")
//  println(s"isDeleted: $isDeleted")
//  println(s"createdBy: $createdBy")
//  println(s"createdAt: $createdAt")
//  println(s"updatedAt: $updatedAt")
//
//  println(s"userProfileOneId (state.id): $userProfileOneId")
//  println(s"userProfileOneName (state.name): $userProfileOneName")
//  println(s"userProfileOneExternalId (state.externalId): $userProfileOneExternalId")
//
//  println(s"userProfileTwoId (district.id): $userProfileTwoId")
//  println(s"userProfileTwoName (district.name): $userProfileTwoName")
//  println(s"userProfileTwoExternalId (district.externalId): $userProfileTwoExternalId")
//
//  println(s"userProfileThreeId (block.id): $userProfileThreeId")
//  println(s"userProfileThreeName (block.name): $userProfileThreeName")
//  println(s"userProfileThreeExternalId (block.externalId): $userProfileThreeExternalId")
//
//  println(s"userProfileFourId (cluster.id): $userProfileFourId")
//  println(s"userProfileFourName (cluster.name): $userProfileFourName")
//  println(s"userProfileFourExternalId (cluster.externalId): $userProfileFourExternalId")
//
//  println(s"userProfileFiveId (school.id): $userProfileFiveId")
//  println(s"userProfileFiveName (school.name): $userProfileFiveName")
//  println(s"userProfileFiveExternalId (school.externalId): $userProfileFiveExternalId")
//





  //  def name: String = readOrDefault[String]("name", null)
//
//  def status: String = readOrDefault[String]("status", null)
//
//  def isDeleted: Boolean = readOrDefault[Boolean]("deleted", false)
//
//  def createdBy: Int = readOrDefault[Int]("createdby", -1)
//
//  def createdAt: Timestamp
//
//  def updatedAt: Timestamp
//
//  def userProfileOneId: String =  readOrDefault[String]("state.id", null)
//
//  def userProfileOneName: String =  readOrDefault[String]("state.name", null)
//
//  def userProfileOneExternalId: String = readOrDefault[String]("state.externalId", null)
//
//  def userProfileTwoId: String = readOrDefault[String]("district.id", null)
//
//  def userProfileTwoName: String = readOrDefault[String]("district.name", null)
//
//  def userProfileTwoExternalId: String = readOrDefault[String]("district.externalId", null)
//
//  def userProfileThreeId: String = readOrDefault[String]("block.id", null)
//
//  def userProfileThreeName: String = readOrDefault[String]("block.name", null)
//
//  def userProfileThreeExternalId: String = readOrDefault[String]("block.externalId", null)
//
//  def userProfileFourId: String = readOrDefault[String]("cluster.id", null)
//
//  def userProfileFourName: String = readOrDefault[String]("cluster.name", null)
//
//  def userProfileFourExternalId: String = readOrDefault[String]("cluster.externalId", null)
//
//  def userProfileFiveId: String = readOrDefault[String]("school.id", null)
//
//  def userProfileFiveName: String = readOrDefault[String]("school.name", null)
//
//  def userProfileFiveExternalId: String = readOrDefault[String]("school.externalId", null)










//  def organizations: List[Map[String, Any]] = {
//    val fromDefault = Option(readOrDefault[List[Map[String, Any]]]("organizations", null)).getOrElse(List.empty)
//    val fromOld     = Option(readOrDefault[List[Map[String, Any]]]("oldValues.organizations", null)).getOrElse(List.empty)
//    val fromNew     = Option(readOrDefault[List[Map[String, Any]]]("newValues.organizations", null)).getOrElse(List.empty)
//
//    (fromDefault ++ fromOld ++ fromNew).distinct
//  }
//
//
//
//
//  def extractField[T](path: String, includeNew: Boolean = false): Option[T] = {
//    val fromDefault = Option(readOrDefault[T](path, null.asInstanceOf[T]))
//    val fromOld     = Option(readOrDefault[T](s"oldValues.$path", null.asInstanceOf[T]))
//    val fromNew     = if (includeNew) Option(readOrDefault[T](s"newValues.$path", null.asInstanceOf[T])) else None
//
//    (fromDefault orElse fromOld orElse fromNew).filter( != null)
//  }
//
//
//
//  //  def organizations: List[Map[String, Any]] =
////    Option(readOrDefault[List[Map[String, Any]]]("organizations", null))
////      .filter(.nonEmpty)
////      .orElse(
////        Option(readOrDefault[List[Map[String, Any]]]("oldValues.organizations", null)).filter(.nonEmpty)
////      )
////      .getOrElse(
////        readOrDefault[List[Map[String, Any]]]("newValues.organizations", List.empty)
////      )
//
//
//  //  def organizations: List[Map[String, Any]] = readOrDefault[List[Map[String, Any]]]("organizations", List.empty)
////  def organizations: List[Map[String, Any]] =
////    Option(readOrDefault[List[Map[String, Any]]]("organizations", null))
////      .filter(.nonEmpty)
////      .getOrElse(readOrDefault[List[Map[String, Any]]]("oldValues.organizations", List.empty))
//
////      def organizations: List[Map[String, Any]] =
////        Option(readOrDefault[List[Map[String, Any]]]("organizations", null)).filter(.nonEmpty)
////          .getOrElse(readOrDefault[List[Map[String, Any]]]("newValues.organizations", List.empty))
//
//  def organizationsid: Int = readOrDefault[Int]("organizations.id", 0)
////  def organizationsid: String = readOrDefault[Int]("organizations.id", 0).toString
//
//  def organizationsname: String = readOrDefault[String]("organizations.name", null)
//
////  def roles: List[Map[String, Any]] = readOrDefault[List[Map[String, Any]]]("roles", List.empty)
//
//
////  def professionalrole: List[Map[String, Any]] = {
////    val fromDefault = Option(readOrDefault[List[Map[String, Any]]]("professionalrole", null)).getOrElse(List.empty)
////    val fromOld     = Option(readOrDefault[List[Map[String, Any]]]("oldValues.professionalrole", null)).getOrElse(List.empty)
////    val fromNew     = Option(readOrDefault[List[Map[String, Any]]]("newValues.professionalrole", null)).getOrElse(List.empty)
////
////    (fromDefault ++ fromOld ++ fromNew).distinct
////  }
//
////  def professionalroleid: String = readOrDefault[String]("professionalrole.id", null)
//  def professionalroleid: String = {
//    Option(readOrDefault[String]("newValues.professionalrole.id", null))
//      .orElse(Option(readOrDefault[String]("oldValues.professionalrole.id", null)))
//      .orElse(Option(readOrDefault[String]("professionalrole.id", null)))
//      .getOrElse("")
//  }
//  def professionalrolename: String = {
//    Option(readOrDefault[String]("newValues.professionalrole.name", null))
//      .orElse(Option(readOrDefault[String]("oldValues.professionalrole.name", null)))
//      .orElse(Option(readOrDefault[String]("professionalrole.name", null)))
//      .getOrElse("")
//  }
//
////  def professionalrolename: String = readOrDefault[String]("professionalrole.name", null)
////  def professionalsubroles: String = readOrDefault[String]("professionalsubroles", null)
//  def professionalsubroles: List[Map[String, Any]] = {
//    val fromDefault = Option(readOrDefault[List[Map[String, Any]]]("professionalsubroles", null)).getOrElse(List.empty)
//    val fromOld     = Option(readOrDefault[List[Map[String, Any]]]("oldValues.professionalsubroles", null)).getOrElse(List.empty)
//    val fromNew     = Option(readOrDefault[List[Map[String, Any]]]("newValues.professionalsubroles", null)).getOrElse(List.empty)
//
//    (fromDefault ++ fromOld ++ fromNew).distinct
//  }
//
//  def professionalsubrolesid: String = readOrDefault[String]("professionalsubroles.id", null)
//
//  def professionalsubrolesname: String = readOrDefault[String]("professionalsubroles.name", null)
//
//
//
////  def programName: String = readOrDefault("meta.programInformation.name", null)
//
////  def createdAt: Timestamp = {
////    val dateString = readOrDefault[String]("createdat", null)
////    if (dateString == null || dateString.trim.isEmpty) {
////      new Timestamp(System.currentTimeMillis())
////    } else {
////      try {
////        Timestamp.from(Instant.parse(dateString))
////      } catch {
////        case : Exception =>
////          val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
////          new Timestamp(formatter.parse(dateString).getTime)
////      }
////    }
////  }
////
////
////  def updatedAt: Timestamp = {
////    val dateString = readOrDefault[String]("updatedat", null)
////    if (dateString == null || dateString.trim.isEmpty) {
////      new Timestamp(System.currentTimeMillis())
////    } else {
////      try {
////        Timestamp.from(Instant.parse(dateString))
////      } catch {
////        case : Exception =>
////          val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
////          new Timestamp(formatter.parse(dateString).getTime)
////      }
////    }
////  }
//
////  def getTimestampFromMaps(
////                            data: Map[String, Any],
////                            keys: List[String]
////                          ): Timestamp = {
////    val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
////
////    keys.toStream
////      .flatMap { key =>
////        data.get(key) match {
////          case Some(ts: Timestamp) => Some(ts)
////          case Some(s: String) if s.trim.nonEmpty =>
////            try Some(Timestamp.from(Instant.parse(s)))
////            catch {
////              case : Exception =>
////                try Some(new Timestamp(formatter.parse(s).getTime))
////                catch { case : Exception => None }
////            }
////          case  => None
////        }
////      }
////      .headOption
////      .getOrElse(new Timestamp(System.currentTimeMillis()))
////  }
////
////  def createdat(data: Map[String, Any]): Timestamp = getTimestampFromMaps(
////    data,
////    List("createdat", "oldValues.createdat", "newValues.createdat")
////  )
////
////  def updatedat(data: Map[String, Any]): Timestamp = getTimestampFromMaps(
////    data,
////    List("updatedat", "oldValues.updatedat", "newValues.updatedat")
////  )
//
//  def createdat: Timestamp = {
//    read("createdat") match {
//      case Some(ts: Timestamp) => ts
//      case Some(value) =>
//        val s = value.toString.trim
//        if (s.isEmpty) new Timestamp(System.currentTimeMillis())
//        else {
//          try {
//            Timestamp.from(Instant.parse(s))
//          } catch {
//            case : Exception =>
//              val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//              new Timestamp(formatter.parse(s).getTime)
//          }
//        }
//      case  => new Timestamp(System.currentTimeMillis())
//    }
//  }
//
//
//  def updatedat: Timestamp = {
//    read("updatedat") match {
//      case Some(ts: Timestamp) => ts
//      case Some(value) =>
//        val s = value.toString.trim
//        if (s.isEmpty) new Timestamp(System.currentTimeMillis())
//        else {
//          try {
//            Timestamp.from(Instant.parse(s))
//          } catch {
//            case : Exception =>
//              val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//              new Timestamp(formatter.parse(s).getTime)
//          }
//        }
//      case  => new Timestamp(System.currentTimeMillis())
//    }
//  }





}

