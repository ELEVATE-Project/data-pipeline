package org.shikshalokam.user.mapping.stream.processor.fixture

object ObservationEventsMock {

  // Sample observation submission event matching the expected format
  val OBSERVATION_SUBMITTED: String = """{
    "eventType": "observation-submitted",
    "id": 3088,
    "organizationId": 1,
    "observationData": {
      "name": "Carol Miranda Updated Two",
      "about": "admin Update",
      "dob": "22-12-1990"
    }
  }"""

  // Additional test case with minimal data (only name)
  val OBSERVATION_SUBMITTED_MINIMAL: String = """{
    "eventType": "observation-submitted",
    "id": 3088,
    "organizationId": 2,
    "observationData": {
      "name": "Test Student Name"
    }
  }"""

  // Test case with only about field
  val OBSERVATION_SUBMITTED_ABOUT_ONLY: String = """{
    "eventType": "observation-submitted",
    "id": 3089,
    "organizationId": 3,
    "observationData": {
      "about": "Updated about information"
    }
  }"""

}
