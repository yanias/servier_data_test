FROM python:3.9-slim

WORKDIR /servier_data_test

COPY . /servier_data_test

# Install any needed packages specified in requirements.txt
RUN pip3 install --no-cache-dir -r requirements.txt

# Make port 80 available to the world outside this container
EXPOSE 80

# Run app.py when the container launches
CMD ["python", "-m", "src.app.py"]
