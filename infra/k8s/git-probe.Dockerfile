FROM spark:4.0.2-scala2.13-java21-python3-ubuntu@sha256:ddf38559b2520671eb73a66a0c43185cc9e6b015aa944fcac32e5e05f610edd5
USER root
RUN apt-get update && apt-cache madison git
