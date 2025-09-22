
# build admin tool

./gradlew   :polaris-admin:assemble   :polaris-admin:quarkusAppPartsBuild --rerun   -Dquarkus.container-image.build=true


# deploy
kubectl create namespace polaris


kubectl -n polaris create secret generic gcp-creds-secret   --from-file=helm/polaris/gcs.json

kubectl create secret generic polaris-persistence -n polaris \
--from-literal=username=iceberg \
--from-literal=password=1Nvrw8In6Kz14ri9cv \
--from-literal=jdbcUrl='jdbc:postgresql://shared-postgresql17.qe2.conviva.com:5000/POLARIS'




helm upgrade --install --namespace polaris \
--values helm/polaris/ci/persistence-values.yaml \
polaris helm/polaris



kubectl wait --namespace polaris --for=condition=ready pod --selector=app.kubernetes.io/name=polaris --timeout=120s




# Bootstrapping Realms and Principal Credentials 

java  -Dquarkus.datasource.password=1Nvrw8In6Kz14ri9cv -Dquarkus.datasource.username=iceberg -Dquarkus.datasource.db-kind=postgresql  -Dquarkus.datasource.jdbc.url=jdbc:postgresql://shared-postgresql17.qe2.conviva.com:5000/POLARIS -jar runtime/admin/build/quarkus-app/quarkus-run.jar bootstrap -r POLARIS -c POLARIS,root,s3cr3t



# create catalog 
./polaris --host  10.12.70.124  --port 30881   \
--client-id root \
--client-secret s3cr3t \
catalogs create dpi_catalog \
--type INTERNAL \
--storage-type gcs \
--default-base-location gs://conviva-prod-datalake/dpi-catalog \
--allowed-location gs://conviva-prod-datalake/dpi-catalog \
--service-account platform-storage-prod-proc-svc@platform-storage-prod-0-533d.iam.gserviceaccount.com \
--property environment=dev \
--property team=data-platform


# delete 
helm uninstall --namespace polaris polaris
kubectl delete --namespace polaris -f helm/polaris/ci/fixtures/

kubectl delete namespace polaris



# access control

./polaris --host 10.12.70.124 --port 30881 --client-id root --client-secret s3cr3t catalog-roles create --catalog dpi_catalog dpi_admin

./polaris --host 10.12.70.124 --port 30881 --client-id root  --client-secret s3cr3t   principal-roles   create  dpi_admin_role

./polaris  --host 10.12.70.124 --port 30881 --client-id root  --client-secret s3cr3t  principal-roles   grant   --principal root   dpi_admin_role

./polaris --host 10.12.70.124 --port 30881 --client-id root --client-secret s3cr3t catalog-roles grant --catalog dpi_catalog --principal-role dpi_admin_role dpi_admin

./polaris --host 10.12.70.124 --port 30881 --client-id root --client-secret s3cr3t privileges catalog grant --catalog dpi_catalog --catalog-role dpi_admin CATALOG_MANAGE_CONTENT



# spark sql

bin/spark-sql \
--driver-memory 18g \
--executor-memory 18g \
--packages \
org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.0, \
org.apache.iceberg:iceberg-gcp-bundle:1.9.0, \
com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.18 \
--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
--conf spark.sql.catalog.polaris.warehouse=dpi_catalog \
--conf spark.sql.catalog.polaris.header.X-Iceberg-Access-Delegation=vended-credentials \
--conf spark.sql.catalog.polaris=org.apache.iceberg.spark.SparkCatalog \
--conf spark.sql.catalog.polaris.catalog-impl=org.apache.iceberg.rest.RESTCatalog \
--conf spark.sql.catalog.polaris.uri=http://10.12.70.124:30881/api/catalog/ \
--conf spark.sql.catalog.polaris.credential='root:s3cr3t' \
--conf spark.sql.catalog.polaris.scope='PRINCIPAL_ROLE:ALL' \
--conf spark.sql.defaultCatalog=polaris \
--conf spark.sql.catalog.polaris.token-refresh-enabled=true \
--conf spark.sql.catalog.polaris.client.region=us-east-4 \
--conf spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem \
--conf spark.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS \
--conf spark.hadoop.google.cloud.auth.service.account.enable=true \
--conf spark.hadoop.google.cloud.auth.service.account.json.keyfile=/root/polaris/gcs-sa.json



# pyspark

bin/pyspark  --driver-memory 32g --executor-memory 40g --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.0,org.apache.iceberg:iceberg-gcp-bundle:1.9.0,com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.18 --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.catalog.dpi_catalog.warehouse=dpi_catalog --conf spark.sql.catalog.dpi_catalog.header.X-Iceberg-Access-Delegation=vended-credentials --conf spark.sql.catalog.dpi_catalog=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.dpi_catalog.catalog-impl=org.apache.iceberg.rest.RESTCatalog --conf spark.sql.catalog.dpi_catalog.uri=http://10.12.70.124:30881/api/catalog/ --conf spark.sql.catalog.dpi_catalog.credential='root:s3cr3t' --conf spark.sql.catalog.dpi_catalog.scope='PRINCIPAL_ROLE:ALL' --conf spark.sql.defaultCatalog=dpi_catalog --conf spark.sql.catalog.dpi_catalog.token-refresh-enabled=true --conf spark.sql.catalog.dpi_catalog.client.region=us-east-4 --conf spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem --conf spark.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS --conf spark.hadoop.google.cloud.auth.service.account.enable=true --conf spark.hadoop.google.cloud.auth.service.account.json.keyfile=/conviva/data/nvme12n1/datalake/polaris-1.0/polaris/gcs-sa.json --conf spark.local.dir=/conviva/data/nvme12n1/datalake/sparktmp

# create table 

import runpy
runpy.run_path("/root/spark-3.5.5-bin-hadoop3/eco_page.py", run_name="__main__")






# remove orphan file

CALL polaris.system.expire_snapshots('default.eco_page_flow_pt1m_dist', TIMESTAMP '2025-09-18 08:54:00',2);

CALL  polaris.system.remove_orphan_files(table => 'default.eco_page_flow_pt1m_dist', location => 'gs://conviva-prod-datalake/dpi-catalog/default/eco_page_flow_pt1m_dist/data' , dry_run => TRUE);