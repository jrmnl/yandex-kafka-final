package codecs

import (
	"log"

	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	avroserde "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/avro"
)

type Avro[T any] struct {
	serializer   *avroserde.GenericSerializer
	deserializer *avroserde.GenericDeserializer
	topic        string
}

func NewAvroSerializer[T any](config *schemaregistry.Config, topic string) *Avro[T] {
	client, err := schemaregistry.NewClient(config)
	if err != nil {
		log.Fatalf("Ошибка при подключении к Schema Registry: %v", err)
	}

	serCfg := avroserde.NewSerializerConfig()
	serCfg.AutoRegisterSchemas = true
	serializer, err := avroserde.NewGenericSerializer(client, serde.ValueSerde, serCfg)
	if err != nil {
		log.Fatalf("Ошибка при создании сериализатора: %v", err)
	}

	deserializer, err := avroserde.NewGenericDeserializer(client, serde.ValueSerde, avroserde.NewDeserializerConfig())
	if err != nil {
		log.Fatalf("Ошибка при создании десериализатора: %v", err)
	}

	return &Avro[T]{
		serializer:   serializer,
		deserializer: deserializer,
		topic:        topic,
	}
}

func (s *Avro[T]) Encode(value interface{}) ([]byte, error) {
	bytes, err := s.serializer.Serialize(s.topic, value)
	if err != nil {
		return nil, err
	}
	return bytes, nil
}

func (s *Avro[T]) Decode(data []byte) (interface{}, error) {
	var msg T
	err := s.deserializer.DeserializeInto(s.topic, data, &msg)
	return msg, err
}
