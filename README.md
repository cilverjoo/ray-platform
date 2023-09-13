## ray 클러스터 생성

#### kuberay operator 설치

	kubectl create -k "github.com/ray-project/kuberay/ray-operator/config/default?ref=v0.5.0&timeout=90s"


#### ray cluster 설치

	kubectl create namespace ray-cluster-1
	kubectl create namespace ray-cluster-2

	kubectl create -f ray/ray-cluster.autoscaler.yaml -n ray-cluster-1
	kubectl create -f ray/ray-cluster.autoscaler.yaml -n ray-cluster-2


* 별도의 head node를 갖는 서로 다른 레이 클러스터를 각 네임스페이스에 생성한다.
* ray-cluster yaml 파일에 수정이 있을 경우, apply는 적용이 되지 않기 때문에 지우고 다시 생성해야 한다.


### ray-cluster의 배포
* 현재까지는 ray-cluster의 설정에 변경사항이 있는 경우, ray-cluster를 삭제하고 다시 생성해야 한다.
* ArgoCD를 활용해서 모든 resource를 제거하고 다시 배포하도록 설정할 수 있으나, 이런 경우 ray-cluster가 잠시 중단되는 문제가 발생한다.
* 대안은 dependency만 설치된 이미지를 생성한 다음, git-sync를 활용해서 코드의 수정/추가만 있는 경우를 커버하는 것이다.


### ray_template
* ray 공식문서에 나와있는 [Pattern: Using pipelining to increase throughput](https://docs.ray.io/en/latest/ray-core/patterns/pipelining.html)과 [Pattern: Using a supervisor actor to manage a tree of actors](https://docs.ray.io/en/latest/ray-core/patterns/tree-of-actors.html)를 참고했다.
* RayProcess를 RayWorker를 감독하는 supervisor actor로서 만들었으며, RayWorker에서 RayActorError가 발생하면 계속 진행하고 그 외의 오류가 Raise 됐을 때 kill을 호출하여 actor를 종료시킨다.
* RayWorker는 PathQueue와 같이 작업할 path 등을 가지고 있는 Queue에서 작업을 가져와서 ray.get에 넣어 순서대로 수행한다. 이 때 supervisor actor에 의해 모든 Worker가 같은 Queue를 할당받는다.
* Queue가 모두 소모되었을 때 Worker에서 작업된 task의 수 및 작업된 Pod의 주소 등을 리턴하고 종료한다.
* ray dashboard가 발전되면서, 이제는 Actor 별 task 로그 및 Actor가 완료한 task의 수 등을 별다른 작업 없이 대시보드에서 모니터링 할 수 있게 되었다.


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


* 첫 설치 시, airflow web으로 연결했을 때 Variable이 values.yaml 안에 선언되지 않아 오류가 발생할 수 있다. 
* 계정의 role을 Admin으로 준 다음, airflow-web > Variable로 들어가 직접 추가해주면 된다. 


### airflow에서 dags로 ray job 실행시키기
* 이미지에서 실행할 코드를 cli로 실행시킬 수 있게 환경을 만들어둔 후, 이미지의 depencency를 갖춘 ray 전용 이미지에 작업을 제출한다.
* ray job submit --address="http://<RAY_CLUSTER_EXTERNAL_ADDRESS>:8265" --working-dir="<코드스페이스의 root 경로>" -- CLI
* ray job을 제출하는 기본 dags를 만들어 두고, 코드에 따라 별도의 cli를 날리도록 새로운 인자를 받을 수 있게 설정한다.
* ray-cluster를 복수 개 띄워두고 작업이 진행중이지 않은 cluster만 선택하려는 경우, ray에서 제공하는 JobSubmissionClient를 사용하여 실행중인 job 리스트를 조회할 수 있다.


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

