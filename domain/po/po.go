package po

func DeepCopyPipelineInfo(source *PipelineInfo) *PipelineInfo {
	p := &PipelineInfo{
		Id:                    source.Id,
		Name:                  source.Name,
		SourceId:              source.SourceId,
		EndpointId:            source.EndpointId,
		EndpointType:          source.EndpointType,
		CreateTime:            source.CreateTime,
		Status:                source.Status,
		StreamBulkSize:        source.StreamBulkSize,
		StreamFlushInterval:   source.StreamFlushInterval,
		BatchCoroutines:       source.BatchCoroutines,
		BatchBulkSize:         source.BatchBulkSize,
		AlarmMailList:         source.AlarmMailList,
		AlarmWebhook:          source.AlarmWebhook,
		AlarmWebhookSecretKey: source.AlarmWebhookSecretKey,
		AlarmItemList:         source.AlarmItemList,
	}
	return p
}

func DeepCopyRule(source *Rule) *Rule {
	p := &Rule{
		Type:                         source.Type,
		ReceiveType:                  source.ReceiveType,
		Schema:                       source.Schema,
		Table:                        source.Table,
		TableList:                    source.TableList,
		TablePattern:                 source.TablePattern,
		TableType:                    source.TableType,
		ColumnNameFormatter:          source.ColumnNameFormatter,
		ExcludeColumnList:            source.ExcludeColumnList,
		ColumnNameMapping:            source.ColumnNameMapping,
		AdditionalColumnValueMapping: source.AdditionalColumnValueMapping,
		DataEncoder:                  source.DataEncoder,
		DataExpression:               source.DataExpression,
		DateFormatter:                source.DateFormatter,
		DatetimeFormatter:            source.DatetimeFormatter,
		ReserveCoveredData:           source.ReserveCoveredData,
		OrderColumn:                  source.OrderColumn,
		RedisStructure:               source.RedisStructure,
		RedisKeyPrefix:               source.RedisKeyPrefix,
		RedisKeyBuilder:              source.RedisKeyBuilder,
		RedisKeyColumn:               source.RedisKeyColumn,
		RedisKeyExpression:           source.RedisKeyExpression,
		RedisKeyFixValue:             source.RedisKeyFixValue,
		RedisHashFieldPrefix:         source.RedisHashFieldPrefix,
		RedisHashFieldColumn:         source.RedisHashFieldColumn,
		RedisSortedSetScoreColumn:    source.RedisSortedSetScoreColumn,
		MongodbDatabase:              source.MongodbDatabase,
		MongodbCollection:            source.MongodbCollection,
		EsIndexBuildType:             source.EsIndexBuildType,
		EsIndexName:                  source.EsIndexName,
		EsIndexMappings:              source.EsIndexMappings,
		MqTopic:                      source.MqTopic,
		HttpParameterName:            source.HttpParameterName,
		HttpReserveRawData:           source.HttpReserveRawData,
		LuaScript:                    source.LuaScript,
		Enable:                       source.Enable,
	}
	return p
}
