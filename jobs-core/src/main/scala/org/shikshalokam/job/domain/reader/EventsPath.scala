package org.shikshalokam.job.domain.reader

object EventsPath {
  val METADATA_PATH = "metadata"
  val FLAGS_PATH = "flags"
  val ETS_PATH = "ets"
  val TIMESTAMP_PATH = "ts"
  val MID_PATH = "mid"
  val EID_PATH = "eid"
  val DEVICE_DATA_PATH = "devicedata"
  val USERDATA_PATH = "userdata"
  val CONTENT_DATA_PATH = "contentdata"
  val DIAL_CODE_PATH = "dialcodedata"
  val COLLECTION_PATH = "collectiondata"
  val VERSION_KEY_PATH = "ver"
  val EDATA_PATH = "edata"
  val EDATA_DIR_PATH = s"$EDATA_PATH.dir"
  val EDATA_TYPE_PATH = s"$EDATA_PATH.type"
  val EDTA_FILTERS = s"$EDATA_PATH.filters"
  val EDATA_ITEM = s"$EDATA_PATH.items"
  val EDATA_LOCATION_PATH = s"$EDATA_PATH.loc"
  val CHECKSUM_PATH = "metadata.checksum"
  val DERIVED_LOCATION_PATH = "derivedlocationdata"
  val STATE_KEY_PATH = "state"
  val DISTRICT_KEY_PATH = "district"
  val LOCATION_DERIVED_FROM_PATH = "from"
  val TIMESTAMP = "@timestamp"
  val CONTEXT_PATH = "context"
  val CONTEXT_P_DATA_PATH = s"$CONTEXT_PATH.pdata"
  val CONTEXT_P_DATA_PID_PATH = s"$CONTEXT_P_DATA_PATH.pid"
  val CONTEXT_P_DATA_ID_PATH = s"$CONTEXT_P_DATA_PATH.id"
  val CONTEXT_DID_PATH = s"$CONTEXT_PATH.did"
  val CONTEXT_SID_PATH = s"$CONTEXT_PATH.sid"
  val CONTEXT_ENV_PATH = s"$CONTEXT_PATH.env"
  val CONTEXT_ROLLUP_PATH = s"$CONTEXT_PATH.rollup"
  val CONTEXT_CDATA = s"$CONTEXT_PATH.cdata"
  val DIMENSION_PATH = "dimensions"
  val DIMENSION_CHANNEL_PATH = s"$DIMENSION_PATH.channel"
  val DIMENSION_DID_PATH = s"$DIMENSION_PATH.did"
  val CONTEXT_CHANNEL_PATH = s"$CONTEXT_PATH.channel"
  val UID_PATH = "uid"
  val ACTOR_PATH = "actor"
  val ACTOR_ID_PATH = s"$ACTOR_PATH.id"
  val ACTOR_TYPE_PATH = s"$ACTOR_PATH.type"
  val OBJECT_PATH = "object"
  val OBJECT_ID_PATH = s"$OBJECT_PATH.id"
  val OBJECT_TYPEPATH = s"$OBJECT_PATH.type"
  val OBJECT_ROLLUP_L1 = s"$OBJECT_PATH.rollup.l1"
  val SYNC_TS_PATH ="syncts"
  val TAGS_PATH ="tags"
  val OBJECT_ROLLUP_L2 = s"$OBJECT_PATH.rollup.l2"
  val L2_DATA_PATH = "l2data"
}