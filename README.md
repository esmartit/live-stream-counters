# live-stream-counters

kubectl run test-kafka-client --rm --tty -i --restart='Never' --image docker.io/bitnami/kafka:2.5.0-debian-10-r1 --namespace default --command -- bash

kafka-topics.sh --list --bootstrap-server test-kafka.default.svc.cluster.local:9092

kafka-topics.sh --create --bootstrap-server event-bus-kafka:9092 --replication-factor 3 --partitions 16 --topic smartpoke-device-presence
kafka-topics.sh --create --bootstrap-server event-bus-kafka:9092 --replication-factor 3 --partitions 16 --topic smartpoke-registered-users
kafka-topics.sh --create --bootstrap-server event-bus-kafka:9092 --replication-factor 3 --partitions 16 --topic smartpoke-session-activity
kafka-topics.sh --create --bootstrap-server event-bus-kafka:9092 --replication-factor 3 --partitions 16 --topic smartpoke-unique-devices-detected-count
kafka-topics.sh --create --bootstrap-server event-bus-kafka:9092 --replication-factor 3 --partitions 16 --topic smartpoke-hourly-presence-count
kafka-topics.sh --create --bootstrap-server event-bus-kafka:9092 --replication-factor 3 --partitions 8 --topic smartpoke-sensor-settings --config cleanup.policy=compact
kafka-topics.sh --create --bootstrap-server event-bus-kafka:9092 --replication-factor 3 --partitions 16 --topic smartpoke-device-position
kafka-topics.sh --create --bootstrap-server event-bus-kafka:9092 --replication-factor 3 --partitions 16 --topic smartpoke-daily-unique-devices-detected-count

kafka-topics.sh --delete --bootstrap-server event-bus-kafka:9092 --topic test
kafka-topics.sh --describe --bootstrap-server event-bus-kafka:9092 --topic smartpoke-device-presence