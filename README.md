# live-stream-counters

Release 1.6.1

kubectl run test-kafka-client --rm --tty -i --restart='Never' --image docker.io/bitnami/kafka:2.5.0-debian-10-r1 --namespace default --command -- bash

kafka-topics.sh --list --bootstrap-server event-bus-kafka:9092

kafka-topics.sh --create --bootstrap-server event-bus-kafka:9092 --replication-factor 3 --partitions 16 --topic smartpoke-device-presence
kafka-topics.sh --create --bootstrap-server event-bus-kafka:9092 --replication-factor 3 --partitions 16 --topic smartpoke-registered-users
kafka-topics.sh --create --bootstrap-server event-bus-kafka:9092 --replication-factor 3 --partitions 16 --topic smartpoke-session-activity
kafka-topics.sh --create --bootstrap-server event-bus-kafka:9092 --replication-factor 3 --partitions 16 --topic smartpoke-unique-devices-detected-count
kafka-topics.sh --create --bootstrap-server event-bus-kafka:9092 --replication-factor 3 --partitions 16 --topic smartpoke-hourly-presence-count
kafka-topics.sh --create --bootstrap-server event-bus-kafka:9092 --replication-factor 3 --partitions 8 --topic smartpoke-sensor-settings --config cleanup.policy=compact
kafka-topics.sh --create --bootstrap-server event-bus-kafka:9092 --replication-factor 3 --partitions 16 --topic smartpoke-device-position
kafka-topics.sh --create --bootstrap-server event-bus-kafka:9092 --replication-factor 3 --partitions 16 --topic smartpoke-daily-unique-devices-detected-count
kafka-topics.sh --create --bootstrap-server event-bus-kafka:9092 --replication-factor 3 --partitions 16 --topic smartpoke-minute-presence-count
kafka-topics.sh --create --bootstrap-server event-bus-kafka:9092 --replication-factor 3 --partitions 16 --topic smartpoke-minute-unique-devices-detected-count
kafka-topics.sh --create --bootstrap-server event-bus-kafka:9092 --replication-factor 3 --partitions 16 --topic smartpoke-registered-users-count

kafka-topics.sh --delete --bootstrap-server event-bus-kafka:9092 --topic test
kafka-topics.sh --describe --bootstrap-server event-bus-kafka:9092 --topic smartpoke-device-presence

release 2.0.0

kafka-streams-application-reset.sh --application-id smartpoke-live-stream-counters --bootstrap-servers event-bus-kafka:9092 --input-topics smartpoke-daily-unique-devices-detected-count,smartpoke-daily-unique-devices-detected-count-input,smartpoke-device-position,smartpoke-device-presence,smartpoke-hourly-presence-count,smartpoke-live-stream-counters-daily-unique-devices-detected-count-store-changelog,smartpoke-live-stream-counters-daily-unique-devices-detected-count-store-repartition,smartpoke-live-stream-counters-daily-unique-devices-detected-store-changelog,smartpoke-live-stream-counters-minute-unique-devices-detected-count-store-changelog,smartpoke-live-stream-counters-minute-unique-devices-detected-count-store-repartition,smartpoke-live-stream-counters-minute-unique-devices-detected-store-changelog,smartpoke-minute-presence-count,smartpoke-minute-unique-devices-detected-count,smartpoke-registered-users,smartpoke-registered-users-count,smartpoke-sensor-settings,smartpoke-session-activity,smartpoke-unique-devices-detected-count


kafka-topics.sh --delete --bootstrap-server event-bus-kafka:9092 --topic smartpoke-hourly-presence-count
kafka-topics.sh --delete --bootstrap-server event-bus-kafka:9092 --topic smartpoke-minute-presence-count
kafka-topics.sh --delete --bootstrap-server event-bus-kafka:9092 --topic smartpoke-daily-unique-devices-detected-count
kafka-topics.sh --delete --bootstrap-server event-bus-kafka:9092 --topic smartpoke-minute-unique-devices-detected-count
kafka-topics.sh --delete --bootstrap-server event-bus-kafka:9092 --topic smartpoke-daily-unique-devices-detected-count-input


kafka-streams-application-reset.sh --application-id smartpoke-live-stream-counters --bootstrap-servers event-bus-kafka:9092 --input-topics DailyUniqueDevicesDetectedConsumer-process-applicationId-daily-unique-devices-detected-count-store-changelog,DailyUniqueDevicesDetectedConsumer-process-applicationId-daily-unique-devices-detected-count-store-repartition,DailyUniqueDevicesDetectedConsumer-process-applicationId-daily-unique-devices-detected-store-changelog,HourlyDevicePresenceConsumer-process-applicationId-HourlyDevicePresenceStore-changelog,HourlyDevicePresenceConsumer-process-applicationId-HourlyDevicePresenceStore-repartition,HourlyDevicePresenceConsumer-process-applicationId-hourly-device-presence-store-changelog,MinuteDevicePresenceConsumer-process-applicationId-MinuteDevicePresenceStore-changelog,MinuteDevicePresenceConsumer-process-applicationId-MinuteDevicePresenceStore-repartition,MinuteDevicePresenceConsumer-process-applicationId-minute-device-presence-store-changelog,MinuteUniqueDevicesDetectedConsumer-process-applicationId-minute-unique-devices-detected-count-store-changelog,MinuteUniqueDevicesDetectedConsumer-process-applicationId-minute-unique-devices-detected-count-store-repartition,MinuteUniqueDevicesDetectedConsumer-process-applicationId-minute-unique-devices-detected-store-changelog,smartpoke-device-position,smartpoke-device-presence,smartpoke-registered-users,smartpoke-registered-users-count,smartpoke-sensor-settings,smartpoke-session-activity,smartpoke-unique-devices-detected-count
