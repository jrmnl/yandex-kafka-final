package main

import (
	"log"

	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	avroserde "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/avro"
)

type Serializer[T any] struct {
	serializer   *avroserde.GenericSerializer
	deserializer *avroserde.GenericDeserializer
	topic        string
}

func NewSerializer[T any](config *schemaregistry.Config, topic string) *Serializer[T] {
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

	return &Serializer[T]{
		serializer:   serializer,
		deserializer: deserializer,
		topic:        topic,
	}
}

func (s *Serializer[T]) Serialize(p T) []byte {
	bytes, err := s.serializer.Serialize(s.topic, p)
	if err != nil {
		log.Fatalf("Не удалось сериализовать сообщение: %v", err)
	}
	return bytes
}

func (s *Serializer[T]) Deserialize(data []byte) (T, error) {
	var msg T
	err := s.deserializer.DeserializeInto(s.topic, data, &msg)
	return msg, err
}
