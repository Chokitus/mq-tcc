terraform {
  required_providers {
    pulsar = {
      source = "quantummetric/pulsar"
      version = "1.0.0"
    }
  }
}

provider "pulsar" {
  web_service_url = "http://localhost:8080"
}

resource "pulsar_topic" "anycast-topic" {
  tenant = "public"
  namespace = "default"
  topic_type = "persistent"
  topic_name = "anycast-topic"
  partitions = 0
}