FROM hashicorp/terraform:1.7 AS terraform

FROM python:3.12-slim

COPY --from=terraform /bin/terraform /usr/local/bin/terraform

RUN pip install --no-cache-dir awscli

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENTRYPOINT ["/bin/bash"]
