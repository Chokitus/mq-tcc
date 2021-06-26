terraform {
  required_providers {
    rabbitmq = {
      source = "cyrilgdn/rabbitmq"
      version = "1.5.1"
    }
  }
}

provider "rabbitmq" {
  endpoint = "http://127.0.0.1:15672"
  username = "mqtest"
  password = "mqtest"
}

# Anycast Benchmark
resource "rabbitmq_queue" "anycast_queue" {
  name = "anycast_queue"
  settings {
    durable = true
    auto_delete = false
  }
}