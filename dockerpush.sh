#docker stop registry
#docker rm registry
#docker container prune

make clean
make image_bins
make images

docker run -d -p 5000:5000 --restart=always --name registry registry:2
docker push  172.30.0.49:5000/vc-webhook-manager:latest
docker push  172.30.0.49:5000/vc-controller-manager:latest
docker push  172.30.0.49:5000/vc-scheduler:latest

make generate-yaml
