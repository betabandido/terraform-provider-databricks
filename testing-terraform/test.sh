#!/usr/bin/env bash

#rm terraform.tfstate crash.log
export TF_LOG_PATH=testprovider.log
export TF_LOG=DEBUG
rm $TF_LOG_PATH
terraform init && terraform plan && terraform apply