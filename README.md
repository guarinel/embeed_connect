# embeed_connect
# RUN LOCAL
*docker build -t aws-kpl-producer:latest .

*sudo docker run -p 3306 --env-file my-env.txt aws-kpl-producer:latest


 ## ECR
*aws configure

*aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 586481947513.dkr.ecr.us-east-1.amazonaws.com // Your account id

*docker build -t aws-kpl-producer .

*docker tag aws-kpl-producer 586481947513.dkr.ecr.us-east-1.amazonaws.com/kafka-components // your repository

*docker push 586481947513.dkr.ecr.us-east-1.amazonaws.com/kafka-components
