FROM deeplabcut/deeplabcut:latest

ENV LOCAL_PROJECT_PATH=/usr/local/deeplabcut/DLC
ENV config_path=/usr/local/deeplabcut/DLC/config.yaml

RUN pip install boto3
RUN mkdir -p /usr/local/deeplabcut/DLC
WORKDIR /usr/local
ADD dlc-train.py .
CMD ["dlc-train.py"]
ENTRYPOINT ["python"]

