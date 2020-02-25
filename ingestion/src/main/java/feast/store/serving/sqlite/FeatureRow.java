package feast.store.serving.sqlite;

import feast.core.FeatureSetProto.EntitySpec;
import feast.core.FeatureSetProto.FeatureSet;
import feast.storage.RedisProto.RedisKey;
import feast.storage.RedisProto.RedisKey.Builder;
import feast.types.FeatureRowProto;
import feast.types.FieldProto.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class FeatureRow {
  public static RedisKey getKey(FeatureRowProto.FeatureRow featureRow,
      Map<String, FeatureSet> featureSets) {
    FeatureSet featureSet = featureSets.get(featureRow.getFeatureSet());
    List<String> entityNames =
        featureSet.getSpec().getEntitiesList().stream()
            .map(EntitySpec::getName)
            .sorted()
            .collect(Collectors.toList());

    Map<String, Field> entityFields = new HashMap<>();
    Builder redisKeyBuilder = RedisKey.newBuilder().setFeatureSet(featureRow.getFeatureSet());
    for (Field field : featureRow.getFieldsList()) {
      if (entityNames.contains(field.getName())) {
        entityFields.putIfAbsent(
            field.getName(),
            Field.newBuilder().setName(field.getName()).setValue(field.getValue()).build());
      }
    }
    for (String entityName : entityNames) {
      redisKeyBuilder.addEntities(entityFields.get(entityName));
    }
    return redisKeyBuilder.build();
  }
}
