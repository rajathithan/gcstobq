FROM apache/beam_python3.9_sdk:2.49.0

# Prebuilt python dependencies
RUN pip install xmltodict
# Prebuilt other dependencies
RUN apt-get update  

# Set the entrypoint to the Apache Beam SDK launcher.
ENTRYPOINT ["/opt/apache/beam/boot"]