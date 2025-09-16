
# deploy
kubectl create namespace polaris


kubectl -n polaris create secret generic gcp-creds-secret   --from-file=helm/polaris/gcs.json

kubectl create secret generic polaris-persistence -n polaris \
--from-literal=username=admin \
--from-literal=password=admin \
--from-literal=jdbcUrl='jdbc:postgresql://rccp605-3.iad7.prod.conviva.com:5432/POLARIS'




helm upgrade --install --namespace polaris \
--values helm/polaris/ci/persistence-values.yaml \
polaris helm/polaris



kubectl wait --namespace polaris --for=condition=ready pod --selector=app.kubernetes.io/name=polaris --timeout=120s






# delete 
helm uninstall --namespace polaris polaris
kubectl delete --namespace polaris -f helm/polaris/ci/fixtures/

kubectl delete namespace polaris

