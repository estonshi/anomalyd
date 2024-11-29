FROM python:3.10

WORKDIR /opt

COPY . /opt/

RUN pip install -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple

ENTRYPOINT ["python", "main.py"]