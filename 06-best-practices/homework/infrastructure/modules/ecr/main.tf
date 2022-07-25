terraform {
  required_providers {
    docker = {
      source  = "kreuzwerker/docker"
      version = "2.19.0"
    }
  }
}

provider "docker" {
  host = "unix:///var/run/docker.sock"
}

# Pulls the image
resource "docker_image" "alpine" {
  name = "alpine:latest"

}

# Create a container
resource "docker_container" "foo" {
  image = docker_image.alpine.latest
  name  = "foo"
  command = [ "/bin/sleep", "600" ]

}

resource "docker_image" "lambda_docker" {
  name = "model_duration"
  build {
    path = "../../code"
    tag  = [ "${var.ecr_repo_name}:${var.ecr_image_tag}" ]
  }
}


# In practice, the Image build-and-push step is handled separately by the CI/CD pipeline and not the IaC script.
# But because the lambda config would fail without an existing Image URI in ECR,
# we can also upload any base image to bootstrap the lambda config, unrelated to your Inference logic
# resource null_resource ecr_image {
#    triggers = {
#      python_file = md5(file(var.lambda_function_local_path))
#      docker_file = md5(file(var.docker_image_local_path))
#    }

#    provisioner "local-exec" {
#      command = <<EOF
#             #  aws ecr get-login-password --region ${var.region} | docker login --username AWS --password-stdin ${var.account_id}.dkr.ecr.${var.region}.amazonaws.com
#              cd ../
#              docker build -t ${aws_ecr_repository.repo.repository_url}:${var.ecr_image_tag} .
#              docker push ${aws_ecr_repository.repo.repository_url}:${var.ecr_image_tag}
#          EOF
#    }
# }

# // Wait for the image to be uploaded, before lambda config runs
# data aws_ecr_image lambda_image {
#  depends_on = [
#    null_resource.ecr_image
#  ]
#  repository_name = var.ecr_repo_name
#  image_tag       = var.ecr_image_tag
# }

output "image_uri" {
   value     = "s3://stg-mlflow-models-mlops-zoomcamp/stg_stream_model_duration_mlops-zoomcamp.zip" #"${var.ecr_repo_name}:${var.ecr_image_tag}"
}
