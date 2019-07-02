
echo "Start of ${0}"

VERSIONFILE=./VERSION
echo "Version file is ${VERSIONFILE}"
OLDVERSIONNUM=$(cat ${VERSIONFILE})

ecgi "Removing old version ${OLDVERSIONNUM}"
docker rmi metcarob/object_store_abstraction_misc_sqlachemy_debug:${OLDVERSIONNUM}


./bumpVersion.sh ${VERSIONFILE}
RES=$?
if [ ${RES} -ne 0 ]; then
  echo ""
  echo "Bump version failed"
  exit 1
fi
VERSIONNUM=$(cat ${VERSIONFILE})


docker build -t metcarob/object_store_abstraction_misc_sqlachemy_debug:${VERSIONNUM} .

#docker service create --name debugService \
# --secret saas_user_management_system_objectstore_config \
# -e APIAPP_OBJECTSTORECONFIGFILE=/run/secrets/saas_user_management_system_objectstore_config \
# metcarob/object_store_abstraction_misc_sqlachemy_debug:${VERSIONNUM}

 docker service create --name debugService \
  --network main_net \
  --secret local_objectstore_config \
  -e APIAPP_OBJECTSTORECONFIGFILE=/run/secrets/local_objectstore_config \
  metcarob/object_store_abstraction_misc_sqlachemy_debug:${VERSIONNUM}

echo "Waiting 10 seconds to give it a chance to process"
sleep 10

docker service logs debugService
docker service rm debugService

echo "End of ${0}"

exit 0
