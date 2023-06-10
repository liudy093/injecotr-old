FROM python:3.8.5-slim

WORKDIR /usr/local/dag

RUN sed -i 's/deb.debian.org/mirrors.ustc.edu.cn/g' /etc/apt/sources.list && \
    apt update &&\
    apt install -y curl gzip graphviz
    
RUN cd /tmp &&\
    curl -sLO https://ghproxy.com/https://github.com/argoproj/argo-workflows/releases/download/v3.1.1/argo-linux-amd64.gz &&\
    gunzip argo-linux-amd64.gz &&\
    chmod +x argo-linux-amd64 &&\
    mv ./argo-linux-amd64 /bin/argo


COPY requirements.txt ./
RUN pip install -i https://pypi.tuna.tsinghua.edu.cn/simple -r requirements.txt
COPY . .

CMD ["python3", "-u", "injector.py"]