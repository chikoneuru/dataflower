docker stop $(docker ps -q)
docker rm $(docker ps -aq)
sudo docker rmi $(docker images -a -q)
docker rm -f recognizer__upload
docker rm -f recognizer__adult