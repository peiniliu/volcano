bash /home/pliu/opt/go/packages/src/volcano.sh/volcano/installer/dockerfile/webhook-manager/gen-admission-secret.sh --service volcano-admission-service --namespace volcano-system --secret volcano-admission-secret

#grafana admin admin

kubectl port-forward services/grafana 30004:3000 -n volcano-monitoring &
kubectl port-forward services/prometheus-service 30003:8080 -n volcano-monitoring &
kubectl port-forward services/kubernetes-dashboard 30001:443 -n kubernetes-dashboard &


# https://itnext.io/big-change-in-k8s-1-24-about-serviceaccounts-and-their-secrets-4b909a4af4e0
kubectl create serviceaccount dashboard -n default
k create token dashboard -n default
kubectl create clusterrolebinding dashboard-admin -n default --clusterrole=cluster-admin --serviceaccount=default:dashboard

apiVersion: v1
kind: Secret
type: kubernetes.io/service-account-token
metadata:
  name: dashboard
  annotations:
    kubernetes.io/service-account.name: "dashboard"

token:      eyJhbGciOiJSUzI1NiIsImtpZCI6InhEVjNwTW9sTWQzNHFlMFAtYkJ5Tmh4MjZaeldCNUpBQUlfdFJnZ014T0UifQ.eyJpc3MiOiJrdWJlcm5ldGVzL3NlcnZpY2VhY2NvdW50Iiwia3ViZXJuZXRlcy5pby9zZXJ2aWNlYWNjb3VudC9uYW1lc3BhY2UiOiJkZWZhdWx0Iiwia3ViZXJuZXRlcy5pby9zZXJ2aWNlYWNjb3VudC9zZWNyZXQubmFtZSI6ImRhc2hib2FyZCIsImt1YmVybmV0ZXMuaW8vc2VydmljZWFjY291bnQvc2VydmljZS1hY2NvdW50Lm5hbWUiOiJkYXNoYm9hcmQiLCJrdWJlcm5ldGVzLmlvL3NlcnZpY2VhY2NvdW50L3NlcnZpY2UtYWNjb3VudC51aWQiOiI2ZjBhZWM1OC05MDk2LTQzNDItOTFhNy05MDVkZWQ4MjQ4YmEiLCJzdWIiOiJzeXN0ZW06c2VydmljZWFjY291bnQ6ZGVmYXVsdDpkYXNoYm9hcmQifQ.pRNYtEWyM7cLiFON2pYQxeP93pduDmCk38_yiDGPB32a2j8Zb6Bh5irGrML-62cvp9LlEb6g77jFf7l2IEsMvknkFKsp7E0JPkiHaEtRATCMJuMQpfgnbwFPM5Y9iUAd_rbaIsk27QmE1RFE6O73m5S1dJcEZi-BheOpTOY1e7UAvqq273fCxTAuZi2qO07m7qNn599Dv_OBm7kO6TyLV7fZMZMrot3XE9TIvcq7KlRLPH_nI5oFKBqPrTy7IxRpySLKG1nNR2xiFcc_5x3N-gj_mpAXYuBTSkLbUQS-17NnXO76-TvXjOcSsKEgidM3QwCwKRlzvR4pVByxVmlsvA

# intel PCM 
monitor intel processors with the system 