FROM debian:bookworm-20240812
RUN apt update
RUN apt install -y python3=3.11.2-1+b1
RUN apt install -y python3-pip=23.0.1+dfsg-1
RUN python3 -m pip config set global.break-system-packages true
RUN pip install ipdb==0.13.13
RUN pip install PyYAML==6.0.2
RUN pip install spacy==3.7.6
RUN python3 -m spacy download de_core_news_lg 
RUN echo "alias python=python3" >> /root/.bashrc
RUN echo "alias ipdb='python -m ipdb'" >> /root/.bashrc
WORKDIR /veld/code/

