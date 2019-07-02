

docker rmi object_store_abstraction_misc_sqlachemy_debug
docker build -t object_store_abstraction_misc_sqlachemy_debug .

#docker service create --name debugService \
# --secret saas_user_management_system_objectstore_config \
# -e APIAPP_OBJECTSTORECONFIGFILE=/run/secrets/saas_user_management_system_objectstore_config \
# object_store_abstraction_misc_sqlachemy_debug

 docker service create --name debugService \
  --network main_net \
  --secret local_objectstore_config \
  -e APIAPP_OBJECTSTORECONFIGFILE=/run/secrets/local_objectstore_config \
  object_store_abstraction_misc_sqlachemy_debug

echo "Waiting 10 seconds to give it a chance to process"
sleep 10

docker service logs debugService
docker service rm debugService
