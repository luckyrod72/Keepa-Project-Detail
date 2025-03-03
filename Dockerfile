FROM ubuntu:latest
LABEL authors="zhouyang"

ENTRYPOINT ["top", "-b"]