# PYSPARK DECONTAINERS TUTORIALS

This project contains beginner pyspark tutorials.
To run these scripts i built a docker image to use as a devcontainer in vscode, so that i didnt need to install java and spark on my mac.


# REFERENCES

- [docker sparkblog post](https://endjin.com/blog/2025/01/spark-devcontainers-local-spark)

    **IMPORTANT NOTE** : change the Java machine architecture inside Dockerfile

        ARG JAVA_ARCH=arm64 # this is MacOs


- [Philipp Brunenberg pyspark live coding video](https://youtu.be/yzpVFUUj4R4?si=98owRNx8Bd0Cbkbu)

    - [AAPL stock market dataset on kaggle](https://www.kaggle.com/datasets/jacksoncrow/stock-market-dataset)



