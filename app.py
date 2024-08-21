from flask import Flask, request, jsonify, render_template, url_for
import csv
import requests
import threading
import queue
import logging
import json

app = Flask(__name__)

with open('config.json', 'r') as file:
    config = json.load(file)

log_path = config['logging'].get('log_path', 'app.log')

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
file_handler = logging.FileHandler(log_path, mode='w')
file_handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

csv_file = config['csv'].get('file_path', 'data.csv')
num_threads = config['app'].get('thread_size', 10)
queue_size = config['app'].get('queue_size', 10)


def read_csv():
    """
    Read records from the csv file.
    :return: List of records read from the csv file.
    """
    logger.debug("Entering read_csv function.")
    try:
        logger.info(f"Attempting to open file {csv_file} for reading.")
        with open(csv_file, mode='r', newline='') as file:
            reader = csv.DictReader(file)
            logger.info(f"File {csv_file} read successfully.")
            return list(reader)
    except FileNotFoundError as e:
        logger.error(f"FileNotFoundError: {e}")
        return jsonify({"error": f"FileNotFoundError {e}"})


def write_csv(rows):
    """
    write records to the csv file.
    :param rows: List of records to write to the csv file.

    """
    logger.debug("Entering write_csv function.")
    logger.info(f"Attempting to open file {csv_file} for writing.")
    with open(csv_file, mode='w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=["Rollno", "name", "english", "maths", "science"])
        writer.writeheader()
        writer.writerows(rows)
        logger.info(f"File {csv_file} written successfully.")


@app.route('/')
def index():
    """
    Render the home page.

    """
    logger.debug("Rendering home page.")
    return render_template('index.html')


@app.route('/read', methods=['GET'])
def read_record():
    """
    Read a record by Rollno.
    :return: Response: Json object with record data or error message.

    """
    rollno = request.args.get('rollno')
    if rollno:
        logger.debug(f"Received request to read record with rollno {rollno}")
        records = read_csv()
        for record in records:
            if record['Rollno'] == rollno:
                logger.info(f"Record with rollo {rollno} found")
                return record
        logger.warning(f"Record with rollo {rollno} not found")
        return jsonify({"error": f"Record with Rollno {rollno} not found"})
    else:
        return render_template('read.html')


@app.route('/insert', methods=['GET', 'POST'])
def create_record():
    """
    Insert a new record.
    :return: Json object with success or error message.

    """
    if request.method == 'POST':
        data = request.form.to_dict()
        records = read_csv()
        if any(record["Rollno"] == data["Rollno"] for record in records):
            logger.warning(f"Record with {data['Rollno']} already exists")
            return jsonify({"error": f"Record with {data['Rollno']} already exists"}), 400

        records.append(data)
        write_csv(records)
        logger.info(f"Record with Rollno {data['Rollno']} added successfully.")
        return jsonify({"message": "Record added successfully"}), 201
    return render_template('insert.html')


@app.route('/update', methods=['GET', 'POST'])
def update_page():
    """
    Render the update page or handle update requests.
    :return: Json object with success or error message.
    """
    if request.method == 'POST':
        rollno = request.form.get('rollno')
        data = request.form.to_dict()

        try:
            logger.info(f"Received request to update record with Rollno {rollno}")
            response = requests.put(
                url_for('update_record', rollno=rollno, _external=True),
                json=data
            )
            if response.status_code == 200:
                logger.info(f"Record with Rollno {rollno} updated successfully")
                return jsonify({"message": f"Record with Rollno {rollno} updated successfully"})
            elif response.status_code == 404:
                logger.warning(f"Record with Rollno {rollno} not found.")
                return jsonify({"error": f"Record with Rollno {rollno} not found"})
            else:
                logger.error(f"Unexpected error with status code {response.status_code} while updating record.")
                return jsonify({"error": "An unexpected error occurred"}), response.status_code
        except Exception as e:
            logger.error(f"Error making UPDATE request: {e}")
            return jsonify({"error": "An error occurred"}), 500

    return render_template('update.html')


@app.route('/update/<rollno>', methods=['PUT'])
def update_record(rollno):
    """
    Update a record by Rollno.
    :param rollno: Rollno of the record to update.
    :return: Json object with success or error message.

    """
    try:
        data = request.json
        logger.info(f"Updating record with Rollno: {rollno}")
        records = read_csv()
        updated = False

        valid_fields = ["Rollno", "name", "english", "maths", "science"]

        filtered_data = {}
        for key, value in data.items():
            if key in valid_fields:
                filtered_data[key] = value

        for record in records:
            if record["Rollno"] == rollno:
                record.update(filtered_data)
                updated = True
                break

        if not updated:
            logger.warning(f"Record with Rollno {rollno} not found.")
            return jsonify({"error": f"Record with Rollno {rollno} not found"}), 404

        write_csv(records)
        logger.info(f"Record with Rollno {rollno} updated successfully.")
        return jsonify({"message": f"Record with Rollno {rollno} updated successfully"}), 200

    except Exception as e:
        logger.error(f"Error updating record: {e}")
        return jsonify({"error": "An error occurred"}), 500


@app.route('/remove', methods=['GET', 'POST'])
def remove_page():
    """
    Render the remove page.
    :return: Json object with success or error message.

    """
    if request.method == 'POST':
        rollno = request.form.get('rollno')

        try:
            logger.info(f"Received request to delete record with Rollno: {rollno}")
            response = requests.delete(url_for('delete_record', rollno=rollno, _external=True))
            if response.status_code == 200:
                logger.info(f"Record with Rollno {rollno} deleted successfully.")
                return jsonify({"message": "Record deleted successfully"})
            elif response.status_code == 404:
                logger.warning(f"Record with Rollno {rollno} not found foe deletion.")
                return jsonify({"error": f"Record  with Rollno {rollno} not found"})
        except Exception as e:
            logger.error(f"Error making DELETE request: {e}")
            return jsonify({"error": "An error occurred"}), 500

    return render_template('remove.html')


@app.route('/remove/<rollno>', methods=['DELETE'])
def delete_record(rollno):
    """
    Delete a record by Rollno.
    :param rollno: Rollno of the record to delete.
    :return: Success or error message.

    """
    try:
        logger.info(f"Deleting record with Rollno: {rollno}")
        records = read_csv()
        new_records = [record for record in records if record["Rollno"] != rollno]

        if new_records == records:
            logger.warning(f"No record found with Rollno: {rollno}")
            return jsonify({"error": "Record not found"}), 404

        write_csv(new_records)
        logger.info(f"Record with Rollno {rollno} deleted successfully.")
        return jsonify({"message": "Record deleted successfully"}), 200

    except Exception as e:
        logger.error(f"Error deleting record: {e}")
        return jsonify({"error": "An error occurred"}), 500


def student_average(records):
    """
    Calculate the average scores for student.
    :param records: List of student records.
    :return: List of records with calculated average scores.

    """
    averages = []
    for record in records:
        try:
            english = float(record['english'])
            maths = float(record['maths'])
            science = float(record['science'])
            average = round((english + maths + science) / 3, 2)
            averages.append({
                'Rollno': record['Rollno'],
                'name': record['name'],
                'average': average
            })
        except (ValueError, KeyError) as e:
            logger.warning(f"Error processing record {record}: {e}")
            return f"Error processing record {record}: {e}"
    return averages


def worker(q, results, lock):
    """
    Worker thread function to process records and calculate averages.
    :param q: Queue with chunks of records.
    :param results: List to store results.
    :param lock: Lock to synchronize access to results.

    """
    while not q.empty():
        chunk = q.get()
        chunk_results = student_average(chunk)
        with lock:
            results.extend(chunk_results)
        q.task_done()


@app.route('/average', methods=['GET'])
def average_scores():
    """
    Calculate and return the average scores for all students.
    :return: Json object with student averages or error message.
    """
    logger.info("Received request to calculate average scores.")
    records = read_csv()
    if not records:
        logger.warning("No records found for average calculations.")
        return jsonify({'error': 'No records found'}), 404

    q = queue.Queue()
    num_threads = 10
    chunk_size = max(len(records) // num_threads, 1)

    for i in range(0, len(records), chunk_size):
        q.put(records[i:i + chunk_size])

    results = []
    lock = threading.Lock()
    threads = []

    for _ in range(num_threads):
        t = threading.Thread(target=worker, args=(q, results, lock))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    if not results:
        logger.warning("No results computed for average scores")
        return jsonify({'error': 'No results computed'}), 500

    logger.info("Average scores calculated successfully")
    return jsonify({'student_averages': results})


if __name__ == '__main__':
    logger.info("Starting Flask application.")
    app.run(debug=True)
