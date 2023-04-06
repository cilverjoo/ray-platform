## airflow 셋팅

#### airflow와 연동할 azure storage의 연결문자열을 airflow를 설치할 네임스페이스에 secret으로 생성
	kubectl create namespace airflow

	kubectl create secret generic airflow-pod-disk-secret \
	-n airflow \
	--from-literal=azurestorageaccountkey=$STORAGE_KEY \
	--from-literal=azurestorageaccountname=aimmodp


#### helm으로 airflow 설치

	export CHART_VERSION=8.6.1

	helm repo add airflow-stable https://airflow-helm.github.io/charts

	helm install \
	  $RELEASE_NAME \
	  airflow-stable/airflow \
	  --namespace airflow \
	  --version $CHART_VERSION \
	  --values airflow/values.yaml


#### helm으로 설치한 airflow의 config 수정 시, upgrade 적용

	helm upgrade airflow airflow-stable/airflow \
	--namespace airflow \
	-f airflow/values.yaml


* 첫 설치 시, airflow web으로 연결했을 때 GSA_PASSWORD값이 없어 오류가 발생할 수 있다. 이 때, 계정에 role을 Admin으로 준 다음, airflow-web에서 Variable로 들어가 직접 추가해주면 된다. 


## prometheus-stack (prometheus, grafana) 셋팅

	helm repo add prometheus-community https://prometheus-community.github.io/helm-charts

	helm repo update

	helm install prometheus prometheus-community/kube-prometheus-stack \
	--namespace monitoring \
	--create-namespace \
	-f values.yaml


## nginx-ingress 설치

	helm install $RELEASE_NAME ingress-nginx/ingress-nginx -n ingress-basic \
	--set controller.replicaCount=1 \
	--set controller.nodeSelector."kubernetes\.io/os"=linux \
	--set controller.admissionWebhooks.patch.nodeSelector."kubernetes\.io/os"=linux \
	--set controller.service.annotations."service\.beta\.kubernetes\.io/azure-load-balancer-health-probe-request-path"=/healthz \
	--set defaultBackend.nodeSelector."kubernetes\.io/os"=linux \
	--set controller.service.externalTrafficPolicy=Local


#### 고정 ip를 nginx-ingress의 주소로 할당

	az aks show \
	--resource-group myResourceGroup \
	--name myAKSCluster \
	--query nodeResourceGroup -o tsv # MC_myResourceGroup_myAKSCluster_eastus 형태

	az network public-ip create \
	--resource-group MC_myResourceGroup_myAKSCluster_eastus \
	--name myAKSPublicIP \
	--sku Standard \
	--allocation-method static \
	--query publicIp.ipAddress -o tsv

	helm upgrade nginx-ingress ingress-nginx/ingress-nginx \
	  --namespace $NAMESPACE \
	  --set controller.service.annotations."service\.beta\.kubernetes\.io/azure-dns-label-name"=$DNS_LABEL \
	  --set controller.service.loadBalancerIP=$STATIC_IP

* 자세한 방법은 [공식문서](https://learn.microsoft.com/ko-kr/azure/aks/ingress-basic?tabs=azure-cli) 참고.


## ingress에 적용할 cert-manager 설치
	# Label the ingress-basic namespace to disable resource validation
	kubectl label namespace ingress-basic cert-manager.io/disable-validation=true

	# Add the Jetstack Helm repository
	helm repo add jetstack https://charts.jetstack.io

	# Update your local Helm chart repository cache
	helm repo update

	# Install the cert-manager Helm chart
	helm install cert-manager jetstack/cert-manager \
	  --namespace ingress-basic \
	  --set installCRDs=true \
	  --set nodeSelector."kubernetes\.io/os"=linux


## cert-manager 생성

	kubectl apply -f ingress-basic/cert-manager.yaml -n ingress-basic


## airflow, prometheus-stack ingress로 연결하기

	kubectl create -f airflow/airflow-ingress.yaml -n ingress-basic
	kubectl create -f monitoring/prometheus-stack-ingress.yaml -n ingress-basic


## ray 클러스터 생성

	kubectl create namespace ray-cluster-1
	kubectl create namespace ray-cluster-2

	kubectl create -f ray/ray-cluster.autoscaler.yaml -n ray-cluster-1
	kubectl create -f ray/ray-cluster.autoscaler.yaml -n ray-cluster-2

* 별도의 head node를 갖는 서로 다른 레이 클러스터를 각 네임스페이스에 생성한다.
* ray-cluster yaml 파일에 수정이 있을 경우, apply는 적용이 되지 않기 때문에 지우고 다시 생성해야 한다.
