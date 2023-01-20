FROM arm64v8/rust:1.66.1

RUN apt update && apt install -y libopencv-dev clang libclang-dev libleptonica-dev libtesseract-dev tesseract-ocr-eng

RUN rustup default nightly
RUN rustup component add rustfmt
ENV CARGO_BUILD_TARGET_DIR=/root/target