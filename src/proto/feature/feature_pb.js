// source: proto/feature/feature.proto
/**
 * @fileoverview
 * @enhanceable
 * @suppress {missingRequire} reports error on implicit type usages.
 * @suppress {messageConventions} JS Compiler reports an error if a variable or
 *     field starts with 'MSG_' and isn't a translatable message.
 * @public
 */
// GENERATED CODE -- DO NOT EDIT!
/* eslint-disable */
// @ts-nocheck

var jspb = require('google-protobuf');
var goog = jspb;
var global = Function('return this')();

var proto_feature_rule_pb = require('../../proto/feature/rule_pb.js');
goog.object.extend(proto, proto_feature_rule_pb);
var proto_feature_target_pb = require('../../proto/feature/target_pb.js');
goog.object.extend(proto, proto_feature_target_pb);
var proto_feature_variation_pb = require('../../proto/feature/variation_pb.js');
goog.object.extend(proto, proto_feature_variation_pb);
var proto_feature_strategy_pb = require('../../proto/feature/strategy_pb.js');
goog.object.extend(proto, proto_feature_strategy_pb);
var proto_feature_feature_last_used_info_pb = require('../../proto/feature/feature_last_used_info_pb.js');
goog.object.extend(proto, proto_feature_feature_last_used_info_pb);
goog.exportSymbol('proto.bucketeer.feature.Feature', null, global);
goog.exportSymbol('proto.bucketeer.feature.Feature.VariationType', null, global);
goog.exportSymbol('proto.bucketeer.feature.Tag', null, global);
goog.exportSymbol('proto.bucketeer.feature.TagFeatures', null, global);
/**
 * Generated by JsPbCodeGenerator.
 * @param {Array=} opt_data Optional initial data array, typically from a
 * server response, or constructed directly in Javascript. The array is used
 * in place and becomes part of the constructed object. It is not cloned.
 * If no data is provided, the constructed object will be empty, but still
 * valid.
 * @extends {jspb.Message}
 * @constructor
 */
proto.bucketeer.feature.Feature = function(opt_data) {
  jspb.Message.initialize(this, opt_data, 0, -1, proto.bucketeer.feature.Feature.repeatedFields_, null);
};
goog.inherits(proto.bucketeer.feature.Feature, jspb.Message);
if (goog.DEBUG && !COMPILED) {
  /**
   * @public
   * @override
   */
  proto.bucketeer.feature.Feature.displayName = 'proto.bucketeer.feature.Feature';
}
/**
 * Generated by JsPbCodeGenerator.
 * @param {Array=} opt_data Optional initial data array, typically from a
 * server response, or constructed directly in Javascript. The array is used
 * in place and becomes part of the constructed object. It is not cloned.
 * If no data is provided, the constructed object will be empty, but still
 * valid.
 * @extends {jspb.Message}
 * @constructor
 */
proto.bucketeer.feature.TagFeatures = function(opt_data) {
  jspb.Message.initialize(this, opt_data, 0, -1, proto.bucketeer.feature.TagFeatures.repeatedFields_, null);
};
goog.inherits(proto.bucketeer.feature.TagFeatures, jspb.Message);
if (goog.DEBUG && !COMPILED) {
  /**
   * @public
   * @override
   */
  proto.bucketeer.feature.TagFeatures.displayName = 'proto.bucketeer.feature.TagFeatures';
}
/**
 * Generated by JsPbCodeGenerator.
 * @param {Array=} opt_data Optional initial data array, typically from a
 * server response, or constructed directly in Javascript. The array is used
 * in place and becomes part of the constructed object. It is not cloned.
 * If no data is provided, the constructed object will be empty, but still
 * valid.
 * @extends {jspb.Message}
 * @constructor
 */
proto.bucketeer.feature.Tag = function(opt_data) {
  jspb.Message.initialize(this, opt_data, 0, -1, null, null);
};
goog.inherits(proto.bucketeer.feature.Tag, jspb.Message);
if (goog.DEBUG && !COMPILED) {
  /**
   * @public
   * @override
   */
  proto.bucketeer.feature.Tag.displayName = 'proto.bucketeer.feature.Tag';
}

/**
 * List of repeated fields within this message type.
 * @private {!Array<number>}
 * @const
 */
proto.bucketeer.feature.Feature.repeatedFields_ = [11,12,13,16];



if (jspb.Message.GENERATE_TO_OBJECT) {
/**
 * Creates an object representation of this proto.
 * Field names that are reserved in JavaScript and will be renamed to pb_name.
 * Optional fields that are not set will be set to undefined.
 * To access a reserved field use, foo.pb_<name>, eg, foo.pb_default.
 * For the list of reserved names please see:
 *     net/proto2/compiler/js/internal/generator.cc#kKeyword.
 * @param {boolean=} opt_includeInstance Deprecated. whether to include the
 *     JSPB instance for transitional soy proto support:
 *     http://goto/soy-param-migration
 * @return {!Object}
 */
proto.bucketeer.feature.Feature.prototype.toObject = function(opt_includeInstance) {
  return proto.bucketeer.feature.Feature.toObject(opt_includeInstance, this);
};


/**
 * Static version of the {@see toObject} method.
 * @param {boolean|undefined} includeInstance Deprecated. Whether to include
 *     the JSPB instance for transitional soy proto support:
 *     http://goto/soy-param-migration
 * @param {!proto.bucketeer.feature.Feature} msg The msg instance to transform.
 * @return {!Object}
 * @suppress {unusedLocalVariables} f is only used for nested messages
 */
proto.bucketeer.feature.Feature.toObject = function(includeInstance, msg) {
  var f, obj = {
    id: jspb.Message.getFieldWithDefault(msg, 1, ""),
    name: jspb.Message.getFieldWithDefault(msg, 2, ""),
    description: jspb.Message.getFieldWithDefault(msg, 3, ""),
    enabled: jspb.Message.getBooleanFieldWithDefault(msg, 4, false),
    deleted: jspb.Message.getBooleanFieldWithDefault(msg, 5, false),
    evaluationUndelayable: jspb.Message.getBooleanFieldWithDefault(msg, 6, false),
    ttl: jspb.Message.getFieldWithDefault(msg, 7, 0),
    version: jspb.Message.getFieldWithDefault(msg, 8, 0),
    createdAt: jspb.Message.getFieldWithDefault(msg, 9, 0),
    updatedAt: jspb.Message.getFieldWithDefault(msg, 10, 0),
    variationsList: jspb.Message.toObjectList(msg.getVariationsList(),
    proto_feature_variation_pb.Variation.toObject, includeInstance),
    targetsList: jspb.Message.toObjectList(msg.getTargetsList(),
    proto_feature_target_pb.Target.toObject, includeInstance),
    rulesList: jspb.Message.toObjectList(msg.getRulesList(),
    proto_feature_rule_pb.Rule.toObject, includeInstance),
    defaultStrategy: (f = msg.getDefaultStrategy()) && proto_feature_strategy_pb.Strategy.toObject(includeInstance, f),
    offVariation: jspb.Message.getFieldWithDefault(msg, 15, ""),
    tagsList: (f = jspb.Message.getRepeatedField(msg, 16)) == null ? undefined : f,
    lastUsedInfo: (f = msg.getLastUsedInfo()) && proto_feature_feature_last_used_info_pb.FeatureLastUsedInfo.toObject(includeInstance, f),
    maintainer: jspb.Message.getFieldWithDefault(msg, 18, ""),
    variationType: jspb.Message.getFieldWithDefault(msg, 19, 0),
    archived: jspb.Message.getBooleanFieldWithDefault(msg, 20, false)
  };

  if (includeInstance) {
    obj.$jspbMessageInstance = msg;
  }
  return obj;
};
}


/**
 * Deserializes binary data (in protobuf wire format).
 * @param {jspb.ByteSource} bytes The bytes to deserialize.
 * @return {!proto.bucketeer.feature.Feature}
 */
proto.bucketeer.feature.Feature.deserializeBinary = function(bytes) {
  var reader = new jspb.BinaryReader(bytes);
  var msg = new proto.bucketeer.feature.Feature;
  return proto.bucketeer.feature.Feature.deserializeBinaryFromReader(msg, reader);
};


/**
 * Deserializes binary data (in protobuf wire format) from the
 * given reader into the given message object.
 * @param {!proto.bucketeer.feature.Feature} msg The message object to deserialize into.
 * @param {!jspb.BinaryReader} reader The BinaryReader to use.
 * @return {!proto.bucketeer.feature.Feature}
 */
proto.bucketeer.feature.Feature.deserializeBinaryFromReader = function(msg, reader) {
  while (reader.nextField()) {
    if (reader.isEndGroup()) {
      break;
    }
    var field = reader.getFieldNumber();
    switch (field) {
    case 1:
      var value = /** @type {string} */ (reader.readString());
      msg.setId(value);
      break;
    case 2:
      var value = /** @type {string} */ (reader.readString());
      msg.setName(value);
      break;
    case 3:
      var value = /** @type {string} */ (reader.readString());
      msg.setDescription(value);
      break;
    case 4:
      var value = /** @type {boolean} */ (reader.readBool());
      msg.setEnabled(value);
      break;
    case 5:
      var value = /** @type {boolean} */ (reader.readBool());
      msg.setDeleted(value);
      break;
    case 6:
      var value = /** @type {boolean} */ (reader.readBool());
      msg.setEvaluationUndelayable(value);
      break;
    case 7:
      var value = /** @type {number} */ (reader.readInt32());
      msg.setTtl(value);
      break;
    case 8:
      var value = /** @type {number} */ (reader.readInt32());
      msg.setVersion(value);
      break;
    case 9:
      var value = /** @type {number} */ (reader.readInt64());
      msg.setCreatedAt(value);
      break;
    case 10:
      var value = /** @type {number} */ (reader.readInt64());
      msg.setUpdatedAt(value);
      break;
    case 11:
      var value = new proto_feature_variation_pb.Variation;
      reader.readMessage(value,proto_feature_variation_pb.Variation.deserializeBinaryFromReader);
      msg.addVariations(value);
      break;
    case 12:
      var value = new proto_feature_target_pb.Target;
      reader.readMessage(value,proto_feature_target_pb.Target.deserializeBinaryFromReader);
      msg.addTargets(value);
      break;
    case 13:
      var value = new proto_feature_rule_pb.Rule;
      reader.readMessage(value,proto_feature_rule_pb.Rule.deserializeBinaryFromReader);
      msg.addRules(value);
      break;
    case 14:
      var value = new proto_feature_strategy_pb.Strategy;
      reader.readMessage(value,proto_feature_strategy_pb.Strategy.deserializeBinaryFromReader);
      msg.setDefaultStrategy(value);
      break;
    case 15:
      var value = /** @type {string} */ (reader.readString());
      msg.setOffVariation(value);
      break;
    case 16:
      var value = /** @type {string} */ (reader.readString());
      msg.addTags(value);
      break;
    case 17:
      var value = new proto_feature_feature_last_used_info_pb.FeatureLastUsedInfo;
      reader.readMessage(value,proto_feature_feature_last_used_info_pb.FeatureLastUsedInfo.deserializeBinaryFromReader);
      msg.setLastUsedInfo(value);
      break;
    case 18:
      var value = /** @type {string} */ (reader.readString());
      msg.setMaintainer(value);
      break;
    case 19:
      var value = /** @type {!proto.bucketeer.feature.Feature.VariationType} */ (reader.readEnum());
      msg.setVariationType(value);
      break;
    case 20:
      var value = /** @type {boolean} */ (reader.readBool());
      msg.setArchived(value);
      break;
    default:
      reader.skipField();
      break;
    }
  }
  return msg;
};


/**
 * Serializes the message to binary data (in protobuf wire format).
 * @return {!Uint8Array}
 */
proto.bucketeer.feature.Feature.prototype.serializeBinary = function() {
  var writer = new jspb.BinaryWriter();
  proto.bucketeer.feature.Feature.serializeBinaryToWriter(this, writer);
  return writer.getResultBuffer();
};


/**
 * Serializes the given message to binary data (in protobuf wire
 * format), writing to the given BinaryWriter.
 * @param {!proto.bucketeer.feature.Feature} message
 * @param {!jspb.BinaryWriter} writer
 * @suppress {unusedLocalVariables} f is only used for nested messages
 */
proto.bucketeer.feature.Feature.serializeBinaryToWriter = function(message, writer) {
  var f = undefined;
  f = message.getId();
  if (f.length > 0) {
    writer.writeString(
      1,
      f
    );
  }
  f = message.getName();
  if (f.length > 0) {
    writer.writeString(
      2,
      f
    );
  }
  f = message.getDescription();
  if (f.length > 0) {
    writer.writeString(
      3,
      f
    );
  }
  f = message.getEnabled();
  if (f) {
    writer.writeBool(
      4,
      f
    );
  }
  f = message.getDeleted();
  if (f) {
    writer.writeBool(
      5,
      f
    );
  }
  f = message.getEvaluationUndelayable();
  if (f) {
    writer.writeBool(
      6,
      f
    );
  }
  f = message.getTtl();
  if (f !== 0) {
    writer.writeInt32(
      7,
      f
    );
  }
  f = message.getVersion();
  if (f !== 0) {
    writer.writeInt32(
      8,
      f
    );
  }
  f = message.getCreatedAt();
  if (f !== 0) {
    writer.writeInt64(
      9,
      f
    );
  }
  f = message.getUpdatedAt();
  if (f !== 0) {
    writer.writeInt64(
      10,
      f
    );
  }
  f = message.getVariationsList();
  if (f.length > 0) {
    writer.writeRepeatedMessage(
      11,
      f,
      proto_feature_variation_pb.Variation.serializeBinaryToWriter
    );
  }
  f = message.getTargetsList();
  if (f.length > 0) {
    writer.writeRepeatedMessage(
      12,
      f,
      proto_feature_target_pb.Target.serializeBinaryToWriter
    );
  }
  f = message.getRulesList();
  if (f.length > 0) {
    writer.writeRepeatedMessage(
      13,
      f,
      proto_feature_rule_pb.Rule.serializeBinaryToWriter
    );
  }
  f = message.getDefaultStrategy();
  if (f != null) {
    writer.writeMessage(
      14,
      f,
      proto_feature_strategy_pb.Strategy.serializeBinaryToWriter
    );
  }
  f = message.getOffVariation();
  if (f.length > 0) {
    writer.writeString(
      15,
      f
    );
  }
  f = message.getTagsList();
  if (f.length > 0) {
    writer.writeRepeatedString(
      16,
      f
    );
  }
  f = message.getLastUsedInfo();
  if (f != null) {
    writer.writeMessage(
      17,
      f,
      proto_feature_feature_last_used_info_pb.FeatureLastUsedInfo.serializeBinaryToWriter
    );
  }
  f = message.getMaintainer();
  if (f.length > 0) {
    writer.writeString(
      18,
      f
    );
  }
  f = message.getVariationType();
  if (f !== 0.0) {
    writer.writeEnum(
      19,
      f
    );
  }
  f = message.getArchived();
  if (f) {
    writer.writeBool(
      20,
      f
    );
  }
};


/**
 * @enum {number}
 */
proto.bucketeer.feature.Feature.VariationType = {
  STRING: 0,
  BOOLEAN: 1,
  NUMBER: 2,
  JSON: 3
};

/**
 * optional string id = 1;
 * @return {string}
 */
proto.bucketeer.feature.Feature.prototype.getId = function() {
  return /** @type {string} */ (jspb.Message.getFieldWithDefault(this, 1, ""));
};


/**
 * @param {string} value
 * @return {!proto.bucketeer.feature.Feature} returns this
 */
proto.bucketeer.feature.Feature.prototype.setId = function(value) {
  return jspb.Message.setProto3StringField(this, 1, value);
};


/**
 * optional string name = 2;
 * @return {string}
 */
proto.bucketeer.feature.Feature.prototype.getName = function() {
  return /** @type {string} */ (jspb.Message.getFieldWithDefault(this, 2, ""));
};


/**
 * @param {string} value
 * @return {!proto.bucketeer.feature.Feature} returns this
 */
proto.bucketeer.feature.Feature.prototype.setName = function(value) {
  return jspb.Message.setProto3StringField(this, 2, value);
};


/**
 * optional string description = 3;
 * @return {string}
 */
proto.bucketeer.feature.Feature.prototype.getDescription = function() {
  return /** @type {string} */ (jspb.Message.getFieldWithDefault(this, 3, ""));
};


/**
 * @param {string} value
 * @return {!proto.bucketeer.feature.Feature} returns this
 */
proto.bucketeer.feature.Feature.prototype.setDescription = function(value) {
  return jspb.Message.setProto3StringField(this, 3, value);
};


/**
 * optional bool enabled = 4;
 * @return {boolean}
 */
proto.bucketeer.feature.Feature.prototype.getEnabled = function() {
  return /** @type {boolean} */ (jspb.Message.getBooleanFieldWithDefault(this, 4, false));
};


/**
 * @param {boolean} value
 * @return {!proto.bucketeer.feature.Feature} returns this
 */
proto.bucketeer.feature.Feature.prototype.setEnabled = function(value) {
  return jspb.Message.setProto3BooleanField(this, 4, value);
};


/**
 * optional bool deleted = 5;
 * @return {boolean}
 */
proto.bucketeer.feature.Feature.prototype.getDeleted = function() {
  return /** @type {boolean} */ (jspb.Message.getBooleanFieldWithDefault(this, 5, false));
};


/**
 * @param {boolean} value
 * @return {!proto.bucketeer.feature.Feature} returns this
 */
proto.bucketeer.feature.Feature.prototype.setDeleted = function(value) {
  return jspb.Message.setProto3BooleanField(this, 5, value);
};


/**
 * optional bool evaluation_undelayable = 6;
 * @return {boolean}
 */
proto.bucketeer.feature.Feature.prototype.getEvaluationUndelayable = function() {
  return /** @type {boolean} */ (jspb.Message.getBooleanFieldWithDefault(this, 6, false));
};


/**
 * @param {boolean} value
 * @return {!proto.bucketeer.feature.Feature} returns this
 */
proto.bucketeer.feature.Feature.prototype.setEvaluationUndelayable = function(value) {
  return jspb.Message.setProto3BooleanField(this, 6, value);
};


/**
 * optional int32 ttl = 7;
 * @return {number}
 */
proto.bucketeer.feature.Feature.prototype.getTtl = function() {
  return /** @type {number} */ (jspb.Message.getFieldWithDefault(this, 7, 0));
};


/**
 * @param {number} value
 * @return {!proto.bucketeer.feature.Feature} returns this
 */
proto.bucketeer.feature.Feature.prototype.setTtl = function(value) {
  return jspb.Message.setProto3IntField(this, 7, value);
};


/**
 * optional int32 version = 8;
 * @return {number}
 */
proto.bucketeer.feature.Feature.prototype.getVersion = function() {
  return /** @type {number} */ (jspb.Message.getFieldWithDefault(this, 8, 0));
};


/**
 * @param {number} value
 * @return {!proto.bucketeer.feature.Feature} returns this
 */
proto.bucketeer.feature.Feature.prototype.setVersion = function(value) {
  return jspb.Message.setProto3IntField(this, 8, value);
};


/**
 * optional int64 created_at = 9;
 * @return {number}
 */
proto.bucketeer.feature.Feature.prototype.getCreatedAt = function() {
  return /** @type {number} */ (jspb.Message.getFieldWithDefault(this, 9, 0));
};


/**
 * @param {number} value
 * @return {!proto.bucketeer.feature.Feature} returns this
 */
proto.bucketeer.feature.Feature.prototype.setCreatedAt = function(value) {
  return jspb.Message.setProto3IntField(this, 9, value);
};


/**
 * optional int64 updated_at = 10;
 * @return {number}
 */
proto.bucketeer.feature.Feature.prototype.getUpdatedAt = function() {
  return /** @type {number} */ (jspb.Message.getFieldWithDefault(this, 10, 0));
};


/**
 * @param {number} value
 * @return {!proto.bucketeer.feature.Feature} returns this
 */
proto.bucketeer.feature.Feature.prototype.setUpdatedAt = function(value) {
  return jspb.Message.setProto3IntField(this, 10, value);
};


/**
 * repeated Variation variations = 11;
 * @return {!Array<!proto.bucketeer.feature.Variation>}
 */
proto.bucketeer.feature.Feature.prototype.getVariationsList = function() {
  return /** @type{!Array<!proto.bucketeer.feature.Variation>} */ (
    jspb.Message.getRepeatedWrapperField(this, proto_feature_variation_pb.Variation, 11));
};


/**
 * @param {!Array<!proto.bucketeer.feature.Variation>} value
 * @return {!proto.bucketeer.feature.Feature} returns this
*/
proto.bucketeer.feature.Feature.prototype.setVariationsList = function(value) {
  return jspb.Message.setRepeatedWrapperField(this, 11, value);
};


/**
 * @param {!proto.bucketeer.feature.Variation=} opt_value
 * @param {number=} opt_index
 * @return {!proto.bucketeer.feature.Variation}
 */
proto.bucketeer.feature.Feature.prototype.addVariations = function(opt_value, opt_index) {
  return jspb.Message.addToRepeatedWrapperField(this, 11, opt_value, proto.bucketeer.feature.Variation, opt_index);
};


/**
 * Clears the list making it empty but non-null.
 * @return {!proto.bucketeer.feature.Feature} returns this
 */
proto.bucketeer.feature.Feature.prototype.clearVariationsList = function() {
  return this.setVariationsList([]);
};


/**
 * repeated Target targets = 12;
 * @return {!Array<!proto.bucketeer.feature.Target>}
 */
proto.bucketeer.feature.Feature.prototype.getTargetsList = function() {
  return /** @type{!Array<!proto.bucketeer.feature.Target>} */ (
    jspb.Message.getRepeatedWrapperField(this, proto_feature_target_pb.Target, 12));
};


/**
 * @param {!Array<!proto.bucketeer.feature.Target>} value
 * @return {!proto.bucketeer.feature.Feature} returns this
*/
proto.bucketeer.feature.Feature.prototype.setTargetsList = function(value) {
  return jspb.Message.setRepeatedWrapperField(this, 12, value);
};


/**
 * @param {!proto.bucketeer.feature.Target=} opt_value
 * @param {number=} opt_index
 * @return {!proto.bucketeer.feature.Target}
 */
proto.bucketeer.feature.Feature.prototype.addTargets = function(opt_value, opt_index) {
  return jspb.Message.addToRepeatedWrapperField(this, 12, opt_value, proto.bucketeer.feature.Target, opt_index);
};


/**
 * Clears the list making it empty but non-null.
 * @return {!proto.bucketeer.feature.Feature} returns this
 */
proto.bucketeer.feature.Feature.prototype.clearTargetsList = function() {
  return this.setTargetsList([]);
};


/**
 * repeated Rule rules = 13;
 * @return {!Array<!proto.bucketeer.feature.Rule>}
 */
proto.bucketeer.feature.Feature.prototype.getRulesList = function() {
  return /** @type{!Array<!proto.bucketeer.feature.Rule>} */ (
    jspb.Message.getRepeatedWrapperField(this, proto_feature_rule_pb.Rule, 13));
};


/**
 * @param {!Array<!proto.bucketeer.feature.Rule>} value
 * @return {!proto.bucketeer.feature.Feature} returns this
*/
proto.bucketeer.feature.Feature.prototype.setRulesList = function(value) {
  return jspb.Message.setRepeatedWrapperField(this, 13, value);
};


/**
 * @param {!proto.bucketeer.feature.Rule=} opt_value
 * @param {number=} opt_index
 * @return {!proto.bucketeer.feature.Rule}
 */
proto.bucketeer.feature.Feature.prototype.addRules = function(opt_value, opt_index) {
  return jspb.Message.addToRepeatedWrapperField(this, 13, opt_value, proto.bucketeer.feature.Rule, opt_index);
};


/**
 * Clears the list making it empty but non-null.
 * @return {!proto.bucketeer.feature.Feature} returns this
 */
proto.bucketeer.feature.Feature.prototype.clearRulesList = function() {
  return this.setRulesList([]);
};


/**
 * optional Strategy default_strategy = 14;
 * @return {?proto.bucketeer.feature.Strategy}
 */
proto.bucketeer.feature.Feature.prototype.getDefaultStrategy = function() {
  return /** @type{?proto.bucketeer.feature.Strategy} */ (
    jspb.Message.getWrapperField(this, proto_feature_strategy_pb.Strategy, 14));
};


/**
 * @param {?proto.bucketeer.feature.Strategy|undefined} value
 * @return {!proto.bucketeer.feature.Feature} returns this
*/
proto.bucketeer.feature.Feature.prototype.setDefaultStrategy = function(value) {
  return jspb.Message.setWrapperField(this, 14, value);
};


/**
 * Clears the message field making it undefined.
 * @return {!proto.bucketeer.feature.Feature} returns this
 */
proto.bucketeer.feature.Feature.prototype.clearDefaultStrategy = function() {
  return this.setDefaultStrategy(undefined);
};


/**
 * Returns whether this field is set.
 * @return {boolean}
 */
proto.bucketeer.feature.Feature.prototype.hasDefaultStrategy = function() {
  return jspb.Message.getField(this, 14) != null;
};


/**
 * optional string off_variation = 15;
 * @return {string}
 */
proto.bucketeer.feature.Feature.prototype.getOffVariation = function() {
  return /** @type {string} */ (jspb.Message.getFieldWithDefault(this, 15, ""));
};


/**
 * @param {string} value
 * @return {!proto.bucketeer.feature.Feature} returns this
 */
proto.bucketeer.feature.Feature.prototype.setOffVariation = function(value) {
  return jspb.Message.setProto3StringField(this, 15, value);
};


/**
 * repeated string tags = 16;
 * @return {!Array<string>}
 */
proto.bucketeer.feature.Feature.prototype.getTagsList = function() {
  return /** @type {!Array<string>} */ (jspb.Message.getRepeatedField(this, 16));
};


/**
 * @param {!Array<string>} value
 * @return {!proto.bucketeer.feature.Feature} returns this
 */
proto.bucketeer.feature.Feature.prototype.setTagsList = function(value) {
  return jspb.Message.setField(this, 16, value || []);
};


/**
 * @param {string} value
 * @param {number=} opt_index
 * @return {!proto.bucketeer.feature.Feature} returns this
 */
proto.bucketeer.feature.Feature.prototype.addTags = function(value, opt_index) {
  return jspb.Message.addToRepeatedField(this, 16, value, opt_index);
};


/**
 * Clears the list making it empty but non-null.
 * @return {!proto.bucketeer.feature.Feature} returns this
 */
proto.bucketeer.feature.Feature.prototype.clearTagsList = function() {
  return this.setTagsList([]);
};


/**
 * optional FeatureLastUsedInfo last_used_info = 17;
 * @return {?proto.bucketeer.feature.FeatureLastUsedInfo}
 */
proto.bucketeer.feature.Feature.prototype.getLastUsedInfo = function() {
  return /** @type{?proto.bucketeer.feature.FeatureLastUsedInfo} */ (
    jspb.Message.getWrapperField(this, proto_feature_feature_last_used_info_pb.FeatureLastUsedInfo, 17));
};


/**
 * @param {?proto.bucketeer.feature.FeatureLastUsedInfo|undefined} value
 * @return {!proto.bucketeer.feature.Feature} returns this
*/
proto.bucketeer.feature.Feature.prototype.setLastUsedInfo = function(value) {
  return jspb.Message.setWrapperField(this, 17, value);
};


/**
 * Clears the message field making it undefined.
 * @return {!proto.bucketeer.feature.Feature} returns this
 */
proto.bucketeer.feature.Feature.prototype.clearLastUsedInfo = function() {
  return this.setLastUsedInfo(undefined);
};


/**
 * Returns whether this field is set.
 * @return {boolean}
 */
proto.bucketeer.feature.Feature.prototype.hasLastUsedInfo = function() {
  return jspb.Message.getField(this, 17) != null;
};


/**
 * optional string maintainer = 18;
 * @return {string}
 */
proto.bucketeer.feature.Feature.prototype.getMaintainer = function() {
  return /** @type {string} */ (jspb.Message.getFieldWithDefault(this, 18, ""));
};


/**
 * @param {string} value
 * @return {!proto.bucketeer.feature.Feature} returns this
 */
proto.bucketeer.feature.Feature.prototype.setMaintainer = function(value) {
  return jspb.Message.setProto3StringField(this, 18, value);
};


/**
 * optional VariationType variation_type = 19;
 * @return {!proto.bucketeer.feature.Feature.VariationType}
 */
proto.bucketeer.feature.Feature.prototype.getVariationType = function() {
  return /** @type {!proto.bucketeer.feature.Feature.VariationType} */ (jspb.Message.getFieldWithDefault(this, 19, 0));
};


/**
 * @param {!proto.bucketeer.feature.Feature.VariationType} value
 * @return {!proto.bucketeer.feature.Feature} returns this
 */
proto.bucketeer.feature.Feature.prototype.setVariationType = function(value) {
  return jspb.Message.setProto3EnumField(this, 19, value);
};


/**
 * optional bool archived = 20;
 * @return {boolean}
 */
proto.bucketeer.feature.Feature.prototype.getArchived = function() {
  return /** @type {boolean} */ (jspb.Message.getBooleanFieldWithDefault(this, 20, false));
};


/**
 * @param {boolean} value
 * @return {!proto.bucketeer.feature.Feature} returns this
 */
proto.bucketeer.feature.Feature.prototype.setArchived = function(value) {
  return jspb.Message.setProto3BooleanField(this, 20, value);
};



/**
 * List of repeated fields within this message type.
 * @private {!Array<number>}
 * @const
 */
proto.bucketeer.feature.TagFeatures.repeatedFields_ = [2];



if (jspb.Message.GENERATE_TO_OBJECT) {
/**
 * Creates an object representation of this proto.
 * Field names that are reserved in JavaScript and will be renamed to pb_name.
 * Optional fields that are not set will be set to undefined.
 * To access a reserved field use, foo.pb_<name>, eg, foo.pb_default.
 * For the list of reserved names please see:
 *     net/proto2/compiler/js/internal/generator.cc#kKeyword.
 * @param {boolean=} opt_includeInstance Deprecated. whether to include the
 *     JSPB instance for transitional soy proto support:
 *     http://goto/soy-param-migration
 * @return {!Object}
 */
proto.bucketeer.feature.TagFeatures.prototype.toObject = function(opt_includeInstance) {
  return proto.bucketeer.feature.TagFeatures.toObject(opt_includeInstance, this);
};


/**
 * Static version of the {@see toObject} method.
 * @param {boolean|undefined} includeInstance Deprecated. Whether to include
 *     the JSPB instance for transitional soy proto support:
 *     http://goto/soy-param-migration
 * @param {!proto.bucketeer.feature.TagFeatures} msg The msg instance to transform.
 * @return {!Object}
 * @suppress {unusedLocalVariables} f is only used for nested messages
 */
proto.bucketeer.feature.TagFeatures.toObject = function(includeInstance, msg) {
  var f, obj = {
    tag: jspb.Message.getFieldWithDefault(msg, 1, ""),
    featuresList: jspb.Message.toObjectList(msg.getFeaturesList(),
    proto.bucketeer.feature.Feature.toObject, includeInstance)
  };

  if (includeInstance) {
    obj.$jspbMessageInstance = msg;
  }
  return obj;
};
}


/**
 * Deserializes binary data (in protobuf wire format).
 * @param {jspb.ByteSource} bytes The bytes to deserialize.
 * @return {!proto.bucketeer.feature.TagFeatures}
 */
proto.bucketeer.feature.TagFeatures.deserializeBinary = function(bytes) {
  var reader = new jspb.BinaryReader(bytes);
  var msg = new proto.bucketeer.feature.TagFeatures;
  return proto.bucketeer.feature.TagFeatures.deserializeBinaryFromReader(msg, reader);
};


/**
 * Deserializes binary data (in protobuf wire format) from the
 * given reader into the given message object.
 * @param {!proto.bucketeer.feature.TagFeatures} msg The message object to deserialize into.
 * @param {!jspb.BinaryReader} reader The BinaryReader to use.
 * @return {!proto.bucketeer.feature.TagFeatures}
 */
proto.bucketeer.feature.TagFeatures.deserializeBinaryFromReader = function(msg, reader) {
  while (reader.nextField()) {
    if (reader.isEndGroup()) {
      break;
    }
    var field = reader.getFieldNumber();
    switch (field) {
    case 1:
      var value = /** @type {string} */ (reader.readString());
      msg.setTag(value);
      break;
    case 2:
      var value = new proto.bucketeer.feature.Feature;
      reader.readMessage(value,proto.bucketeer.feature.Feature.deserializeBinaryFromReader);
      msg.addFeatures(value);
      break;
    default:
      reader.skipField();
      break;
    }
  }
  return msg;
};


/**
 * Serializes the message to binary data (in protobuf wire format).
 * @return {!Uint8Array}
 */
proto.bucketeer.feature.TagFeatures.prototype.serializeBinary = function() {
  var writer = new jspb.BinaryWriter();
  proto.bucketeer.feature.TagFeatures.serializeBinaryToWriter(this, writer);
  return writer.getResultBuffer();
};


/**
 * Serializes the given message to binary data (in protobuf wire
 * format), writing to the given BinaryWriter.
 * @param {!proto.bucketeer.feature.TagFeatures} message
 * @param {!jspb.BinaryWriter} writer
 * @suppress {unusedLocalVariables} f is only used for nested messages
 */
proto.bucketeer.feature.TagFeatures.serializeBinaryToWriter = function(message, writer) {
  var f = undefined;
  f = message.getTag();
  if (f.length > 0) {
    writer.writeString(
      1,
      f
    );
  }
  f = message.getFeaturesList();
  if (f.length > 0) {
    writer.writeRepeatedMessage(
      2,
      f,
      proto.bucketeer.feature.Feature.serializeBinaryToWriter
    );
  }
};


/**
 * optional string tag = 1;
 * @return {string}
 */
proto.bucketeer.feature.TagFeatures.prototype.getTag = function() {
  return /** @type {string} */ (jspb.Message.getFieldWithDefault(this, 1, ""));
};


/**
 * @param {string} value
 * @return {!proto.bucketeer.feature.TagFeatures} returns this
 */
proto.bucketeer.feature.TagFeatures.prototype.setTag = function(value) {
  return jspb.Message.setProto3StringField(this, 1, value);
};


/**
 * repeated Feature features = 2;
 * @return {!Array<!proto.bucketeer.feature.Feature>}
 */
proto.bucketeer.feature.TagFeatures.prototype.getFeaturesList = function() {
  return /** @type{!Array<!proto.bucketeer.feature.Feature>} */ (
    jspb.Message.getRepeatedWrapperField(this, proto.bucketeer.feature.Feature, 2));
};


/**
 * @param {!Array<!proto.bucketeer.feature.Feature>} value
 * @return {!proto.bucketeer.feature.TagFeatures} returns this
*/
proto.bucketeer.feature.TagFeatures.prototype.setFeaturesList = function(value) {
  return jspb.Message.setRepeatedWrapperField(this, 2, value);
};


/**
 * @param {!proto.bucketeer.feature.Feature=} opt_value
 * @param {number=} opt_index
 * @return {!proto.bucketeer.feature.Feature}
 */
proto.bucketeer.feature.TagFeatures.prototype.addFeatures = function(opt_value, opt_index) {
  return jspb.Message.addToRepeatedWrapperField(this, 2, opt_value, proto.bucketeer.feature.Feature, opt_index);
};


/**
 * Clears the list making it empty but non-null.
 * @return {!proto.bucketeer.feature.TagFeatures} returns this
 */
proto.bucketeer.feature.TagFeatures.prototype.clearFeaturesList = function() {
  return this.setFeaturesList([]);
};





if (jspb.Message.GENERATE_TO_OBJECT) {
/**
 * Creates an object representation of this proto.
 * Field names that are reserved in JavaScript and will be renamed to pb_name.
 * Optional fields that are not set will be set to undefined.
 * To access a reserved field use, foo.pb_<name>, eg, foo.pb_default.
 * For the list of reserved names please see:
 *     net/proto2/compiler/js/internal/generator.cc#kKeyword.
 * @param {boolean=} opt_includeInstance Deprecated. whether to include the
 *     JSPB instance for transitional soy proto support:
 *     http://goto/soy-param-migration
 * @return {!Object}
 */
proto.bucketeer.feature.Tag.prototype.toObject = function(opt_includeInstance) {
  return proto.bucketeer.feature.Tag.toObject(opt_includeInstance, this);
};


/**
 * Static version of the {@see toObject} method.
 * @param {boolean|undefined} includeInstance Deprecated. Whether to include
 *     the JSPB instance for transitional soy proto support:
 *     http://goto/soy-param-migration
 * @param {!proto.bucketeer.feature.Tag} msg The msg instance to transform.
 * @return {!Object}
 * @suppress {unusedLocalVariables} f is only used for nested messages
 */
proto.bucketeer.feature.Tag.toObject = function(includeInstance, msg) {
  var f, obj = {
    id: jspb.Message.getFieldWithDefault(msg, 1, ""),
    createdAt: jspb.Message.getFieldWithDefault(msg, 2, 0),
    updatedAt: jspb.Message.getFieldWithDefault(msg, 3, 0)
  };

  if (includeInstance) {
    obj.$jspbMessageInstance = msg;
  }
  return obj;
};
}


/**
 * Deserializes binary data (in protobuf wire format).
 * @param {jspb.ByteSource} bytes The bytes to deserialize.
 * @return {!proto.bucketeer.feature.Tag}
 */
proto.bucketeer.feature.Tag.deserializeBinary = function(bytes) {
  var reader = new jspb.BinaryReader(bytes);
  var msg = new proto.bucketeer.feature.Tag;
  return proto.bucketeer.feature.Tag.deserializeBinaryFromReader(msg, reader);
};


/**
 * Deserializes binary data (in protobuf wire format) from the
 * given reader into the given message object.
 * @param {!proto.bucketeer.feature.Tag} msg The message object to deserialize into.
 * @param {!jspb.BinaryReader} reader The BinaryReader to use.
 * @return {!proto.bucketeer.feature.Tag}
 */
proto.bucketeer.feature.Tag.deserializeBinaryFromReader = function(msg, reader) {
  while (reader.nextField()) {
    if (reader.isEndGroup()) {
      break;
    }
    var field = reader.getFieldNumber();
    switch (field) {
    case 1:
      var value = /** @type {string} */ (reader.readString());
      msg.setId(value);
      break;
    case 2:
      var value = /** @type {number} */ (reader.readInt64());
      msg.setCreatedAt(value);
      break;
    case 3:
      var value = /** @type {number} */ (reader.readInt64());
      msg.setUpdatedAt(value);
      break;
    default:
      reader.skipField();
      break;
    }
  }
  return msg;
};


/**
 * Serializes the message to binary data (in protobuf wire format).
 * @return {!Uint8Array}
 */
proto.bucketeer.feature.Tag.prototype.serializeBinary = function() {
  var writer = new jspb.BinaryWriter();
  proto.bucketeer.feature.Tag.serializeBinaryToWriter(this, writer);
  return writer.getResultBuffer();
};


/**
 * Serializes the given message to binary data (in protobuf wire
 * format), writing to the given BinaryWriter.
 * @param {!proto.bucketeer.feature.Tag} message
 * @param {!jspb.BinaryWriter} writer
 * @suppress {unusedLocalVariables} f is only used for nested messages
 */
proto.bucketeer.feature.Tag.serializeBinaryToWriter = function(message, writer) {
  var f = undefined;
  f = message.getId();
  if (f.length > 0) {
    writer.writeString(
      1,
      f
    );
  }
  f = message.getCreatedAt();
  if (f !== 0) {
    writer.writeInt64(
      2,
      f
    );
  }
  f = message.getUpdatedAt();
  if (f !== 0) {
    writer.writeInt64(
      3,
      f
    );
  }
};


/**
 * optional string id = 1;
 * @return {string}
 */
proto.bucketeer.feature.Tag.prototype.getId = function() {
  return /** @type {string} */ (jspb.Message.getFieldWithDefault(this, 1, ""));
};


/**
 * @param {string} value
 * @return {!proto.bucketeer.feature.Tag} returns this
 */
proto.bucketeer.feature.Tag.prototype.setId = function(value) {
  return jspb.Message.setProto3StringField(this, 1, value);
};


/**
 * optional int64 created_at = 2;
 * @return {number}
 */
proto.bucketeer.feature.Tag.prototype.getCreatedAt = function() {
  return /** @type {number} */ (jspb.Message.getFieldWithDefault(this, 2, 0));
};


/**
 * @param {number} value
 * @return {!proto.bucketeer.feature.Tag} returns this
 */
proto.bucketeer.feature.Tag.prototype.setCreatedAt = function(value) {
  return jspb.Message.setProto3IntField(this, 2, value);
};


/**
 * optional int64 updated_at = 3;
 * @return {number}
 */
proto.bucketeer.feature.Tag.prototype.getUpdatedAt = function() {
  return /** @type {number} */ (jspb.Message.getFieldWithDefault(this, 3, 0));
};


/**
 * @param {number} value
 * @return {!proto.bucketeer.feature.Tag} returns this
 */
proto.bucketeer.feature.Tag.prototype.setUpdatedAt = function(value) {
  return jspb.Message.setProto3IntField(this, 3, value);
};


goog.object.extend(exports, proto.bucketeer.feature);
