FROM debian:bookworm-20240812
RUN apt update
RUN apt install -y python3=3.11.2-1+b1
RUN apt install -y python3-pip=23.0.1+dfsg-1
RUN pip3 install --break-system-packages ipdb==0.13.13

