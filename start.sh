
dockerBuild() {
  docker build -t mq-tests:latest .
}

case $1 in
  prep)
    echo "Creating network"
    docker network create mq-tests-network
    exit
    ;;
  full)
    mvn clean package -T 1C
    dockerBuild
    exit
    ;;
  build)
    dockerBuild
    exit
    ;;
  run)
    docker run -it --network mq-tests-network mq-tests "$@" && \
    echo "Copying test_results to host machine" && \
    docker ps -alq | xargs -I % sh -c 'docker cp %:/test_results .' && \
    echo "Copy successful, deleting docker container" && \
    docker ps -alq | xargs docker rm && \
    echo "Delete successful, exiting..."
    exit
    ;;
  br)
    checksum=$(sha1sum target/mq-tests-1.0.0-SNAPSHOT.jar | cut -c1-40)
    lastchecksum=$(cat lastchecksum.sha1)
    echo "$lastchecksum"
    echo "$checksum"
    if [ "$checksum" != "$lastchecksum" ]; then
      echo "Checksum differed, rebuilding..."
      touch lastchecksum.sha1
      bash -c "echo $checksum > lastchecksum.sha1"
      dockerBuild
    fi
    docker run -it --network mq-tests-network mq-tests "$@" && \
    echo "Copying test_results to host machine" && \
    docker ps -alq | xargs -I % sh -c 'docker cp %:/test_results .' && \
    echo "Copy successful, deleting docker container" && \
    docker ps -alq | xargs docker rm && \
    echo "Delete successful, exiting..."
    exit
    ;;
  *)
    echo "Accepts only build and run"
esac
