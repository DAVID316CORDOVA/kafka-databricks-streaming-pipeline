# -----------------------------------------------------------------------
# NOTA DE TRANSPARENCIA
# -----------------------------------------------------------------------
# Este Terraform documenta como se automatizaria la infraestructura de
# este proyecto (EC2 + Kafka)
# -----------------------------------------------------------------------

terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

variable "aws_region" {
  description = "Region de AWS donde se despliega el EC2"
  type        = string
  default     = "us-east-1"
}

variable "instance_type" {
  description = "Tipo de instancia EC2 para el broker de Kafka"
  type        = string
  default     = "t3.medium"
}

variable "allowed_cidr_kafka" {
  description = "CIDR permitido para el puerto del broker (9092). En produccion, restringir a la IP de salida de Databricks, no 0.0.0.0/0."
  type        = string
  default     = "0.0.0.0/0"
}

variable "key_pair_name" {
  description = "Nombre del key pair EC2 existente, para acceso SSH"
  type        = string
}

# -----------------------------------------------------------------------
# Security Group: puertos 9092 (broker) y 8090 (Kafka UI)
# -----------------------------------------------------------------------
resource "aws_security_group" "kafka_sg" {
  name        = "kafka-streaming-sg"
  description = "Security group para el broker Kafka y Kafka UI"

  ingress {
    description = "Kafka broker"
    from_port   = 9092
    to_port     = 9092
    protocol    = "tcp"
    cidr_blocks = [var.allowed_cidr_kafka]
  }

  ingress {
    description = "Kafka UI"
    from_port   = 8090
    to_port     = 8090
    protocol    = "tcp"
    cidr_blocks = [var.allowed_cidr_kafka]
  }

  ingress {
    description = "SSH"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"] # restringir a la IP propia en un despliegue real
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Project = "kafka-streaming-pipeline"
  }
}

# -----------------------------------------------------------------------
# AMI mas reciente de Ubuntu 22.04, en vez de un ID de AMI fijo
# -----------------------------------------------------------------------
data "aws_ami" "ubuntu" {
  most_recent = true
  owners      = ["099720109477"] # Canonical

  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-*"]
  }
}

# -----------------------------------------------------------------------
# EC2: instala Docker y levanta Kafka + Kafka UI automaticamente al
# arrancar, via user_data -- equivalente a correr setup_kafka.sh a mano.
# -----------------------------------------------------------------------
resource "aws_instance" "kafka_broker" {
  ami                    = data.aws_ami.ubuntu.id
  instance_type          = var.instance_type
  key_name               = var.key_pair_name
  vpc_security_group_ids = [aws_security_group.kafka_sg.id]

  user_data = <<-EOF
    #!/bin/bash
    set -e
    apt-get update
    apt-get install -y docker.io docker-compose-plugin
    systemctl enable docker
    systemctl start docker

    mkdir -p /home/ubuntu/kafka-streaming
    cd /home/ubuntu/kafka-streaming

    cat > docker-compose.yml << 'COMPOSE'
    services:
      kafka:
        image: apache/kafka:latest
        ports:
          - "9092:9092"
        environment:
          KAFKA_NODE_ID: 1
          KAFKA_PROCESS_ROLES: broker,controller
          KAFKA_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://0.0.0.0:9092,CONTROLLER://kafka:29093
          KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://$${PUBLIC_IP}:9092
          KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:29093
          KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
          KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
          KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      kafka-ui:
        image: provectuslabs/kafka-ui:latest
        ports:
          - "8090:8080"
        environment:
          KAFKA_CLUSTERS_0_NAME: ec2
          KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:29092
        depends_on:
          - kafka
    COMPOSE

    docker compose up -d

    sleep 15
    docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
      --create --if-not-exists \
      --topic trades-stream \
      --partitions 3 \
      --replication-factor 1 \
      --bootstrap-server localhost:9092
  EOF

  tags = {
    Name    = "kafka-streaming-broker"
    Project = "kafka-streaming-pipeline"
  }
}

output "kafka_broker_public_ip" {
  description = "IP publica del broker -- usar como BUNDLE_VAR_kafka_bootstrap_servers"
  value       = aws_instance.kafka_broker.public_ip
}

output "kafka_ui_url" {
  value = "http://${aws_instance.kafka_broker.public_ip}:8090"
}
