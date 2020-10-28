NAME := umwrg/hydra-json
TAG	:= $$(git rev-parse --short=8 HEAD)
IMG	:= ${NAME}:${TAG}
LATEST := ${NAME}:latest

build:
	@docker build --force -t ${IMG} .
	@docker tag ${IMG} ${LATEST}
