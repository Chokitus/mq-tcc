case $1 in
  build)
    docker build -t mq-tests:latest .
    exit
    ;;
  run)
    docker run --network mq-tests-network --rm mq-tests "$@"
    exit
    ;;
  *)
    echo "Accepts only build and run"
esac


