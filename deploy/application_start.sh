if [ "$HOSTNAME" == "kafka01" ]; then
  systemctl stop crypto-producer.service 
  systemctl start crypto-producer.service
fi